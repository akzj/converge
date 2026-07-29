package core

import (
	"context"
	"testing"

	"github.com/akzj/converge/pkg/model"
)

func TestEffectBeginEnsurePersistFailureRecoversRevision(t *testing.T) {
	store := &failingExecutionStore{inner: NewMemoryExecutionStore()}
	registry := NewPlanRegistry(store)

	// Install a plan to create the config execution.
	plan, _, err := registry.Install(context.Background(), 0, testPlan(t, "digest", model.Operation{Key: "apply", ExecutionKind: model.ExecutionDirect}))
	if err != nil {
		t.Fatal(err)
	}

	identity := TransitionIdentity{
		EffectIdentity: EffectIdentity{
			EffectID: "test-effect", ReferenceID: "test-ref",
			ConfigID: plan.ConfigID, PlanID: plan.ID, Generation: plan.Generation,
			EffectKey: "download", ProviderType: "test", ProviderDigest: "digest",
		},
		RequestID: "test-ctrl",
	}
	spec := ImmutableEnsureSpec{
		IdempotencyKey: "test", ArtifactID: "sha256:x",
		SemanticFingerprint: "fp", EnsureSpec: []byte(`{"url":"x"}`),
	}

	store.fail = true
	if d, _ := registry.BeginEnsureEffect(context.Background(), BeginEnsureRequest{Identity: identity, Spec: spec}); d != TransitionRejected {
		t.Fatalf("BeginEnsureEffect disposition=%v, want Rejected", d)
	}

	store.fail = false
	if d, err := registry.BeginEnsureEffect(context.Background(), BeginEnsureRequest{Identity: identity, Spec: spec}); err != nil {
		t.Fatal(err)
	} else if d != TransitionApplied {
		t.Fatalf("BeginEnsureEffect disposition=%v, want Applied", d)
	}

	// Verify the effect state is durable.
	loaded := loadEffectSnapshot(t, store, plan.ConfigID)
	if loaded.effects["test-effect"].State != ExternalEffectEnsuring || loaded.references["test-ref"].State != EffectReferenceEnsuring {
		t.Fatalf("effect state not persisted: %#v", loaded.effects)
	}
}

func TestApplyEnsureResultPersistFailureRecoversRevision(t *testing.T) {
	store := &failingExecutionStore{inner: NewMemoryExecutionStore()}
	registry := NewPlanRegistry(store)
	plan, _, err := registry.Install(context.Background(), 0, testPlan(t, "digest", model.Operation{Key: "apply", ExecutionKind: model.ExecutionDirect}))
	if err != nil {
		t.Fatal(err)
	}

	// Begin the ensure.
	identity := TransitionIdentity{
		EffectIdentity: EffectIdentity{
			EffectID: "test-effect-2", ReferenceID: "test-ref-2",
			ConfigID: plan.ConfigID, PlanID: plan.ID, Generation: plan.Generation,
			EffectKey: "download", ProviderType: "test", ProviderDigest: "digest",
		},
		RequestID: "ctrl-ensure",
	}
	spec := ImmutableEnsureSpec{
		IdempotencyKey: "test2", ArtifactID: "sha256:x",
		SemanticFingerprint: "fp", EnsureSpec: []byte(`{"url":"x"}`),
	}
	if d, err := registry.BeginEnsureEffect(context.Background(), BeginEnsureRequest{Identity: identity, Spec: spec}); err != nil || d != TransitionApplied {
		t.Fatalf("Begin: %v %v", d, err)
	}

	// Apply ensure result with failing store.
	store.fail = true
	result := EnsureEffectResult{
		EffectID: "test-effect-2", ReferenceID: "test-ref-2",
		ExternalJobID: "job-1", ExternalRevision: 1,
		Disposition: EnsureBound, Failure: EnsureFailureNone,
	}
	if d, _ := registry.ApplyEnsureResult(context.Background(), identity, result); d != TransitionRejected {
		t.Fatalf("ApplyEnsureResult should be Rejected: %v", d)
	}

	// Verify effect is still Ensuring.
	loaded := loadEffectSnapshot(t, store, plan.ConfigID)
	if loaded.effects["test-effect-2"].State != ExternalEffectEnsuring {
		t.Fatalf("effect state changed after failed persist: %s", loaded.effects["test-effect-2"].State)
	}

	// Retry without failure.
	store.fail = false
	if d, err := registry.ApplyEnsureResult(context.Background(), identity, result); err != nil {
		t.Fatal(err)
	} else if d != TransitionApplied {
		t.Fatalf("ApplyEnsureResult disposition=%v", d)
	}

	// Verify effect is now Active and Bound.
	loaded = loadEffectSnapshot(t, store, plan.ConfigID)
	if loaded.effects["test-effect-2"].State != ExternalEffectActive || loaded.effects["test-effect-2"].ExternalJobID != "job-1" {
		t.Fatalf("effect not active after retry: %#v", loaded.effects)
	}
}

func loadEffectSnapshot(t *testing.T, store *failingExecutionStore, id model.ConfigID) *configExecution {
	t.Helper()
	snapshot, err := store.LoadExecution(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	state := &configExecution{
		effects:    make(map[EffectID]ActiveEffect),
		references: make(map[ReferenceID]EffectReference),
		controls:   make(map[ControlRequestID]EffectControl),
	}
	for _, effect := range snapshot.Effects {
		state.effects[effect.ID] = effect
	}
	for _, ref := range snapshot.EffectReferences {
		state.references[ref.ID] = ref
	}
	for _, ctrl := range snapshot.EffectControls {
		state.controls[ctrl.ID] = ctrl
	}
	return state
}
