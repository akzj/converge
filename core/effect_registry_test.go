package core

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/akzj/converge/pkg/model"
)

func TestPlanRegistryPreservesEffectsAcrossRestoreAndTransition(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryExecutionStore()
	id := model.ConfigID{Name: "config"}
	effect := validEffect()
	reference := EffectReference{ID: "ref", EffectID: effect.ID, ConfigID: id, PlanID: "plan", Generation: 1, EffectKey: "download", State: EffectReferenceActive}
	control := EffectControl{ID: "control", ConfigID: id, ProviderType: effect.ProviderType, ProviderDigest: effect.ProviderDigest, Kind: EffectControlObserve, TargetKind: EffectTargetMaintenance, State: EffectControlPending, EffectID: effect.ID, ReferenceID: reference.ID, NextCheckAt: time.Now()}
	plan := &model.Plan{ID: "plan", ConfigID: id, Generation: 1, Desired: model.DesiredState{ConfigID: id, ProviderType: effect.ProviderType, Version: 1, Digest: model.DesiredSpecDigest(nil)}, ProviderType: effect.ProviderType, ProviderDigest: effect.ProviderDigest, Nodes: map[model.OperationKey]*model.Node{}}
	snapshot := ExecutionSnapshot{Revision: 1, Plan: plan, Effects: []ActiveEffect{effect}, EffectReferences: []EffectReference{reference}, EffectControls: []EffectControl{control}}
	if err := store.CommitExecutionCAS(ctx, id, 0, snapshot); err != nil {
		t.Fatal(err)
	}

	registry := NewPlanRegistry(store)
	if err := registry.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	// An unrelated durable outbox transition must preserve effect state.
	if err := registry.EnqueueOutbox(ctx, model.Event{EventID: "event", ConfigID: id.Name}); err != nil {
		t.Fatal(err)
	}

	recovered := NewPlanRegistry(store)
	if err := recovered.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	recovered.mu.RLock()
	state := recovered.configs[id.Name]
	gotEffect, effectOK := state.effects[effect.ID]
	gotReference, referenceOK := state.references[reference.ID]
	gotControl, controlOK := state.controls[control.ID]
	recovered.mu.RUnlock()
	if !effectOK || !referenceOK || !controlOK {
		t.Fatalf("effect state lost: effect=%v ref=%v control=%v", effectOK, referenceOK, controlOK)
	}
	if string(gotEffect.EnsureSpec) != string(effect.EnsureSpec) || gotReference.EffectID != effect.ID || gotControl.ReferenceID != reference.ID {
		t.Fatalf("effect state changed: %#v %#v %#v", gotEffect, gotReference, gotControl)
	}
}

func TestPlanRegistryRestoreRejectsInvalidEffectSnapshot(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryExecutionStore()
	id := model.ConfigID{Name: "invalid"}
	plan := &model.Plan{ID: "plan", ConfigID: id, Generation: 1, Nodes: map[model.OperationKey]*model.Node{}}
	effect := validEffect()
	effect.EnsureSpec = json.RawMessage(`{"x":1}`)
	invalid := ExecutionSnapshot{Revision: 1, Plan: plan, Effects: []ActiveEffect{effect}, EffectReferences: []EffectReference{{ID: "ref", EffectID: "missing", ConfigID: id, PlanID: plan.ID, Generation: 1, EffectKey: "download", State: EffectReferenceActive}}}
	if err := store.CommitExecutionCAS(ctx, id, 0, invalid); err != nil {
		t.Fatal(err)
	}
	if err := NewPlanRegistry(store).Restore(ctx); err == nil {
		t.Fatal("expected invalid snapshot restore error")
	}
}

func TestCompleteEnsureAndNodePersistsAuthoritativeFailureAtomically(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryExecutionStore()
	registry := NewPlanRegistry(store)
	plan, _, err := registry.Install(ctx, 0, testPlan(t, "digest",
		model.Operation{Key: "ensure", ExecutionKind: model.ExecutionEffectEnsure, EffectKey: "artifact"},
		model.Operation{Key: "observe", ExecutionKind: model.ExecutionEffectObserve, EffectKey: "artifact", DependsOn: []string{"ensure"}},
	))
	if err != nil {
		t.Fatal(err)
	}
	identity := TransitionIdentity{
		EffectIdentity: EffectIdentity{
			EffectID: "effect", ReferenceID: "reference", ConfigID: plan.ConfigID,
			PlanID: plan.ID, Generation: plan.Generation, OperationKey: "ensure",
			EffectKey: "artifact", ProviderType: plan.ProviderType, ProviderDigest: plan.ProviderDigest,
		},
		RequestID: "ensure-effect", AttemptID: "attempt",
	}
	if disposition, err := registry.BeginEnsureEffect(ctx, BeginEnsureRequest{Identity: identity, Spec: ImmutableEnsureSpec{
		IdempotencyKey: "idempotency", ArtifactID: "artifact", SemanticFingerprint: "fingerprint", EnsureSpec: []byte(`{}`),
	}}); err != nil || disposition != TransitionApplied {
		t.Fatalf("begin ensure: disposition=%s err=%v", disposition, err)
	}
	if _, err := registry.ClaimDueControl(ctx, plan.ConfigID, identity.RequestID, time.Now(), identity.AttemptID, "poll", time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	event := model.Event{
		EventID: "attempt/control-result", PlanID: plan.ID, Generation: plan.Generation,
		NodeKey: "ensure", AttemptID: identity.AttemptID, ConfigID: plan.ConfigID.Name,
		State: model.StepFailed, Result: model.StepResult{State: model.StepFailed, Code: "ensure_failed", Reason: "not found"},
	}
	result := EnsureEffectResult{
		EffectID: identity.EffectIdentity.EffectID, ReferenceID: identity.EffectIdentity.ReferenceID,
		Disposition: EnsureFailed, Failure: EnsureFailureAuthoritativeRejected, Code: "ensure_failed", Reason: "not found",
	}
	if disposition, err := registry.CompleteEnsureAndNode(ctx, identity, result, "ensure", event); err != nil || disposition != TransitionApplied {
		t.Fatalf("complete failed ensure: disposition=%s err=%v", disposition, err)
	}

	snapshot := registry.Execution(plan.ConfigID)
	if snapshot.Plan.Nodes["ensure"].Status != model.NodeFailed {
		t.Fatalf("ensure node status=%s, want failed", snapshot.Plan.Nodes["ensure"].Status)
	}
	if len(snapshot.Effects) != 1 || snapshot.Effects[0].State != ExternalEffectFailed || snapshot.Effects[0].ResolutionRequired {
		t.Fatalf("failed effect state=%#v", snapshot.Effects)
	}
	if len(snapshot.EffectControls) != 0 {
		t.Fatalf("terminal ensure control retained: %#v", snapshot.EffectControls)
	}
	if len(snapshot.Outbox) != 1 || snapshot.Outbox[0].State != model.StepFailed {
		t.Fatalf("failure event not persisted: %#v", snapshot.Outbox)
	}

	recovered := NewPlanRegistry(store)
	if err := recovered.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	restored := recovered.Execution(plan.ConfigID)
	if restored.Plan.Nodes["ensure"].Status != model.NodeFailed || len(restored.Outbox) != 1 {
		t.Fatalf("atomic failure lost after restore: %#v", restored)
	}
}
