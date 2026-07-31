package core

import (
	"context"
	"testing"
	"time"

	"github.com/akzj/converge/pkg/model"
)

// setupSingleCASPlan installs a plan with ensure + observe + release nodes and
// returns the installed plan and a registry preloaded with a bound effect and
// a Pending Observe control (via BeginEnsureEffect + ApplyEnsureResult).
func setupSingleCASPlan(t *testing.T, store *MemoryExecutionStore) (*PlanRegistry, *model.Plan, TransitionIdentity) {
	t.Helper()
	ctx := context.Background()
	reg := NewPlanRegistry(store)
	plan, _, err := reg.Install(ctx, 0, testPlan(t, "digest",
		model.Operation{Key: "ensure", ExecutionKind: model.ExecutionEffectEnsure, EffectKey: "download"},
		model.Operation{Key: "observe", ExecutionKind: model.ExecutionEffectObserve, EffectKey: "download", DependsOn: []string{"ensure"}},
		model.Operation{Key: "release", ExecutionKind: model.ExecutionEffectRelease, EffectKey: "download", DependsOn: []string{"observe"}},
	))
	if err != nil {
		t.Fatal(err)
	}
	// beginBoundEffect uses a fresh effect ID/ref ID so BeginEnsureEffect + ApplyEnsureResult
	// create the bound effect and the Observe control.
	identity := beginBoundEffect(t, reg, plan, "single-eff", "single-ref", "job-1")
	// Point the identity at the Observe control created by ApplyEnsureResult.
	identity.RequestID = ControlRequestID("observe-" + string(identity.EffectIdentity.EffectID))
	return reg, plan, identity
}

func claimObserveControl(t *testing.T, reg *PlanRegistry, identity TransitionIdentity) TransitionIdentity {
	t.Helper()
	ctx := context.Background()
	attID, err := newAttemptID()
	if err != nil {
		t.Fatal(err)
	}
	claimed := identity
	claimed.AttemptID = attID
	if _, err := reg.ClaimDueControl(ctx, claimed.EffectIdentity.ConfigID, claimed.RequestID, time.Now(), attID, "poll-1", time.Now().Add(30*time.Second)); err != nil {
		t.Fatal(err)
	}
	return claimed
}

func TestSingleCASObservationCompleted(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryExecutionStore()
	reg, plan, identity := setupSingleCASPlan(t, store)
	identity = claimObserveControl(t, reg, identity)

	event := model.Event{
		EventID: string(identity.AttemptID) + "/control-result",
		PlanID:  plan.ID, Generation: plan.Generation, NodeKey: "observe",
		AttemptID: identity.AttemptID, ConfigID: plan.ConfigID.Name,
		State: model.StepCompleted, Result: model.StepResult{State: model.StepCompleted},
	}
	obs := EffectObservation{
		EffectID: identity.EffectIdentity.EffectID, AttemptID: identity.AttemptID, PollRequestID: "poll-1",
		ExternalJobID: "job-1", ExternalRevision: 2, Disposition: DispositionCompleted,
	}
	disp, err := reg.CompleteEffectObservationAndNode(ctx, identity, obs, "observe", event)
	if err != nil || disp != TransitionApplied {
		t.Fatalf("disp=%v err=%v", disp, err)
	}
	loaded, err := store.LoadExecution(ctx, plan.ConfigID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Plan.Nodes["observe"].Status != model.NodeCompleted {
		t.Fatalf("observe node status=%s, want completed", loaded.Plan.Nodes["observe"].Status)
	}
	found := false
	for _, e := range loaded.Effects {
		if e.ID == identity.EffectIdentity.EffectID && e.State == ExternalEffectCompleted {
			found = true
		}
	}
	if !found {
		t.Fatal("effect not completed")
	}
	outboxFound := false
	for _, ev := range loaded.Outbox {
		if ev.EventID == event.EventID {
			outboxFound = true
		}
	}
	if !outboxFound {
		t.Fatal("outbox event not enqueued in same CAS")
	}
	attemptLeaked := false
	for _, att := range loaded.Attempts {
		if att.ID == identity.AttemptID && att.Status == model.AttemptRunning {
			attemptLeaked = true
		}
	}
	if attemptLeaked {
		t.Fatal("control attempt not retired (still running)")
	}
}

func TestSingleCASObservationFailed(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryExecutionStore()
	reg, plan, identity := setupSingleCASPlan(t, store)
	identity = claimObserveControl(t, reg, identity)

	event := model.Event{
		EventID: string(identity.AttemptID) + "/control-result",
		PlanID:  plan.ID, Generation: plan.Generation, NodeKey: "observe",
		AttemptID: identity.AttemptID, ConfigID: plan.ConfigID.Name,
		State: model.StepFailed, Result: model.StepResult{State: model.StepFailed},
	}
	obs := EffectObservation{
		EffectID: identity.EffectIdentity.EffectID, AttemptID: identity.AttemptID, PollRequestID: "poll-1",
		ExternalJobID: "job-1", ExternalRevision: 2, Disposition: DispositionFailed, Retryable: true,
	}
	disp, err := reg.CompleteEffectObservationAndNode(ctx, identity, obs, "observe", event)
	if err != nil || disp != TransitionApplied {
		t.Fatalf("disp=%v err=%v", disp, err)
	}
	loaded, _ := store.LoadExecution(ctx, plan.ConfigID)
	if loaded.Plan.Nodes["observe"].Status != model.NodeFailed {
		t.Fatalf("observe node status=%s, want failed", loaded.Plan.Nodes["observe"].Status)
	}
	found := false
	for _, e := range loaded.Effects {
		if e.ID == identity.EffectIdentity.EffectID && e.State == ExternalEffectFailed {
			found = true
		}
	}
	if !found {
		t.Fatal("effect not failed")
	}
}

func TestSingleCASObservationAuthoritativeGone(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryExecutionStore()
	reg, plan, identity := setupSingleCASPlan(t, store)
	identity = claimObserveControl(t, reg, identity)

	event := model.Event{
		EventID: string(identity.AttemptID) + "/control-result",
		PlanID:  plan.ID, Generation: plan.Generation, NodeKey: "observe",
		AttemptID: identity.AttemptID, ConfigID: plan.ConfigID.Name,
		State: model.StepCompleted, Result: model.StepResult{State: model.StepCompleted},
	}
	obs := EffectObservation{
		EffectID: identity.EffectIdentity.EffectID, AttemptID: identity.AttemptID, PollRequestID: "poll-1",
		ExternalJobID: "job-1", ExternalRevision: 2, Disposition: DispositionAuthoritativeGone,
	}
	disp, err := reg.CompleteEffectObservationAndNode(ctx, identity, obs, "observe", event)
	if err != nil || disp != TransitionApplied {
		t.Fatalf("disp=%v err=%v", disp, err)
	}
	loaded, _ := store.LoadExecution(ctx, plan.ConfigID)
	for _, e := range loaded.Effects {
		if e.ID == identity.EffectIdentity.EffectID {
			t.Fatal("effect should be removed on authoritative gone")
		}
	}
}

func TestSingleCASReleaseConfirmed(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryExecutionStore()
	reg, plan, identity := setupSingleCASPlan(t, store)
	// Claim the release control (created by beginBoundEffect? No — the release
	// control is created by BeginReleaseEffect). Mark the reference releasing,
	// create the Release control, then claim it.
	releaseIdentity := TransitionIdentity{
		EffectIdentity: identity.EffectIdentity,
		AttemptID:      "rel-att",
		RequestID:      ControlRequestID("release-" + string(identity.EffectIdentity.ReferenceID)),
	}
	if d, err := reg.BeginReleaseEffect(ctx, BeginReleaseRequest{Identity: releaseIdentity}); err != nil || d != TransitionApplied {
		t.Fatalf("BeginRelease: %v %v", d, err)
	}
	if _, err := reg.ClaimDueControl(ctx, plan.ConfigID, releaseIdentity.RequestID, time.Now(), releaseIdentity.AttemptID, "rel-poll", time.Now().Add(30*time.Second)); err != nil {
		t.Fatal(err)
	}
	event := model.Event{
		EventID: string(releaseIdentity.AttemptID) + "/control-result",
		PlanID:  plan.ID, Generation: plan.Generation, NodeKey: "release",
		AttemptID: releaseIdentity.AttemptID, ConfigID: plan.ConfigID.Name,
		State: model.StepCompleted, Result: model.StepResult{State: model.StepCompleted},
	}
	result := ReleaseEffectResult{
		EffectID: identity.EffectIdentity.EffectID, ReferenceID: identity.EffectIdentity.ReferenceID,
		ReleaseRequestID: releaseIdentity.RequestID, ExternalJobID: "job-1", ExternalRevision: 2,
		Disposition: ReleaseConfirmed, Failure: ReleaseFailureNone,
	}
	disp, err := reg.CompleteReleaseAndNode(ctx, releaseIdentity, result, "release", event)
	if err != nil || disp != TransitionApplied {
		t.Fatalf("disp=%v err=%v", disp, err)
	}
	loaded, _ := store.LoadExecution(ctx, plan.ConfigID)
	if loaded.Plan.Nodes["release"].Status != model.NodeCompleted {
		t.Fatalf("release node status=%s, want completed", loaded.Plan.Nodes["release"].Status)
	}
	refFound := false
	for _, r := range loaded.EffectReferences {
		if r.ID == identity.EffectIdentity.ReferenceID && r.State == EffectReferenceReleased {
			refFound = true
		}
	}
	if !refFound {
		t.Fatal("reference not released")
	}
}

func TestSingleCASEnsureReferenceBound(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryExecutionStore()
	reg := NewPlanRegistry(store)
	planV1, _, err := reg.Install(ctx, 0, testPlan(t, "digest",
		model.Operation{Key: "ensure", ExecutionKind: model.ExecutionEffectEnsure, EffectKey: "download"},
		model.Operation{Key: "observe", ExecutionKind: model.ExecutionEffectObserve, EffectKey: "download", DependsOn: []string{"ensure"}},
	))
	if err != nil {
		t.Fatal(err)
	}
	// Bind the effect using the real ensure-op fingerprint so carry matches.
	effID, refID := EffectID("eff-carry"), ReferenceID("ref-carry")
	ensureFp := planV1.Nodes["ensure"].Operation.Fingerprint
	identity := TransitionIdentity{
		EffectIdentity: EffectIdentity{
			EffectID: effID, ReferenceID: refID,
			ConfigID: planV1.ConfigID, PlanID: planV1.ID, Generation: planV1.Generation,
			EffectKey: "download", ProviderType: "test", ProviderDigest: "digest",
		},
		RequestID: ControlRequestID("ensure-" + string(effID)),
	}
	spec := ImmutableEnsureSpec{
		IdempotencyKey: "idem-carry", ArtifactID: planV1.DesiredDigest,
		SemanticFingerprint: ensureFp, EnsureSpec: []byte(`{"url":"x"}`),
	}
	if d, err := reg.BeginEnsureEffect(ctx, BeginEnsureRequest{Identity: identity, Spec: spec}); err != nil || d != TransitionApplied {
		t.Fatalf("BeginEnsure: %v %v", d, err)
	}
	if d, err := reg.ApplyEnsureResult(ctx, identity, EnsureEffectResult{
		EffectID: effID, ReferenceID: refID, ExternalJobID: "job-1", ExternalRevision: 1,
		Disposition: EnsureBound,
	}); err != nil || d != TransitionApplied {
		t.Fatalf("ApplyEnsure Bound: %v %v", d, err)
	}

	// Install a second generation carrying the same EffectKey; transferEffectReferences
	// creates a new Ensuring reference + EnsureReference control for the carry.
	planV2, _, err := reg.Install(ctx, planV1.Generation, testPlan(t, "digest",
		model.Operation{Key: "ensure", ExecutionKind: model.ExecutionEffectEnsure, EffectKey: "download"},
		model.Operation{Key: "observe", ExecutionKind: model.ExecutionEffectObserve, EffectKey: "download", DependsOn: []string{"ensure"}},
	))
	if err != nil {
		t.Fatal(err)
	}

	// Locate the new reference and its EnsureReference control.
	var newRefID ReferenceID
	var ctrlID ControlRequestID
	snapshot, err := store.LoadExecution(ctx, planV1.ConfigID)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range snapshot.EffectReferences {
		if r.Generation == planV2.Generation && r.State == EffectReferenceEnsuring {
			newRefID = r.ID
		}
	}
	for _, c := range snapshot.EffectControls {
		if c.Kind == EffectControlEnsureReference {
			ctrlID = c.ID
		}
	}
	if newRefID == "" || ctrlID == "" {
		t.Fatalf("no carry reference/control: ref=%q ctrl=%q", newRefID, ctrlID)
	}

	attID, _ := newAttemptID()
	refIdentity := TransitionIdentity{
		EffectIdentity: EffectIdentity{
			EffectID: effID, ReferenceID: newRefID,
			ConfigID: planV2.ConfigID, PlanID: planV2.ID, Generation: planV2.Generation,
			EffectKey: "download", ProviderType: "test", ProviderDigest: "digest",
		},
		AttemptID: attID,
		RequestID: ctrlID,
	}
	if _, err := reg.ClaimDueControl(ctx, planV2.ConfigID, ctrlID, time.Now(), attID, "ref-poll", time.Now().Add(30*time.Second)); err != nil {
		t.Fatal(err)
	}
	event := model.Event{
		EventID: string(attID) + "/control-result",
		PlanID:  planV2.ID, Generation: planV2.Generation, NodeKey: "ensure",
		AttemptID: attID, ConfigID: planV2.ConfigID.Name,
		State: model.StepCompleted, Result: model.StepResult{State: model.StepCompleted},
	}
	result := EnsureReferenceResult{
		EffectID: effID, ReferenceID: newRefID,
		RequestID: ctrlID, ExternalJobID: "job-1", ExternalRevision: 2,
		Disposition: EnsureBound, Failure: EnsureFailureNone,
	}
	disp, err := reg.CompleteEnsureReferenceAndNode(ctx, refIdentity, result, "ensure", event)
	if err != nil || disp != TransitionApplied {
		t.Fatalf("disp=%v err=%v", disp, err)
	}
	loaded, _ := store.LoadExecution(ctx, planV2.ConfigID)
	if loaded.Plan.Nodes["ensure"].Status != model.NodeCompleted {
		t.Fatalf("ensure node status=%s, want completed", loaded.Plan.Nodes["ensure"].Status)
	}
	refFound := false
	for _, r := range loaded.EffectReferences {
		if r.ID == newRefID && r.State == EffectReferenceActive {
			refFound = true
		}
	}
	if !refFound {
		t.Fatal("reference not active")
	}
}
