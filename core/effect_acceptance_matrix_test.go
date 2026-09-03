package core

import (
	"context"
	"testing"
	"time"

	"github.com/akzj/converge/pkg/model"
)

// §14 Required test matrix — acceptance gates for External Active Effects.

func beginBoundEffect(t *testing.T, reg *PlanRegistry, plan *model.Plan, effectID EffectID, refID ReferenceID, jobID string) TransitionIdentity {
	t.Helper()
	return beginBoundEffectKey(t, reg, plan, effectID, refID, jobID, "download")
}

func beginBoundEffectKey(t *testing.T, reg *PlanRegistry, plan *model.Plan, effectID EffectID, refID ReferenceID, jobID, effectKey string) TransitionIdentity {
	t.Helper()
	ctx := context.Background()
	identity := TransitionIdentity{
		EffectIdentity: EffectIdentity{
			EffectID: effectID, ReferenceID: refID,
			ConfigID: plan.ConfigID, PlanID: plan.ID, Generation: plan.Generation,
			OperationKey: findEffectOperationKey(plan, effectKey, model.ExecutionEffectEnsure),
			EffectKey:    effectKey, ProviderType: "test", ProviderDigest: "digest",
		},
		RequestID: ControlRequestID("ensure-" + string(effectID)),
	}
	if identity.EffectIdentity.OperationKey == "" {
		identity.EffectIdentity.OperationKey = "apply"
	}
	spec := ImmutableEnsureSpec{
		IdempotencyKey: "idem-" + string(effectID), ArtifactID: "sha256:x",
		SemanticFingerprint: "fp", EnsureSpec: []byte(`{"url":"x"}`),
	}
	if d, err := reg.BeginEnsureEffect(ctx, BeginEnsureRequest{Identity: identity, Spec: spec}); err != nil || d != TransitionApplied {
		t.Fatalf("BeginEnsure: %v %v", d, err)
	}
	if d, err := reg.ApplyEnsureResult(ctx, identity, EnsureEffectResult{
		EffectID: effectID, ReferenceID: refID, ExternalJobID: jobID, ExternalRevision: 1,
		Disposition: EnsureBound,
	}); err != nil || d != TransitionApplied {
		t.Fatalf("ApplyEnsure Bound: %v %v", d, err)
	}
	return identity
}

func TestMatrixEnsureResponseLostRetriesSameJob(t *testing.T) {
	ctx := context.Background()
	service := NewFakeDownloadService()
	provider := NewFakeDownloadProvider("digest", service)
	service.DropNextEnsureResponse = true

	req := newTestEnsureRequest("lost-key", "ref-1")
	first, err := provider.EnsureEffect(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if first.Disposition != EnsureUnknown {
		t.Fatalf("lost response disposition=%s", first.Disposition)
	}
	if len(service.jobs) != 1 {
		t.Fatalf("job should exist after lost response, jobs=%d", len(service.jobs))
	}
	second, err := provider.EnsureEffect(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if second.Disposition != EnsureBound || second.ExternalJobID == "" {
		t.Fatalf("retry: %#v", second)
	}
	if len(service.jobs) != 1 {
		t.Fatalf("retry must reuse job, jobs=%d", len(service.jobs))
	}
}

func TestMatrixLateEnsureAfterDeleteSchedulesRelease(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryExecutionStore()
	reg := NewPlanRegistry(store)
	plan, _, err := reg.Install(ctx, 0, testPlan(t, "digest",
		model.Operation{Key: "ensure", ExecutionKind: model.ExecutionEffectEnsure, EffectKey: "download"},
		model.Operation{Key: "observe", ExecutionKind: model.ExecutionEffectObserve, EffectKey: "download", DependsOn: []string{"ensure"}},
	))
	if err != nil {
		t.Fatal(err)
	}
	identity := TransitionIdentity{
		EffectIdentity: EffectIdentity{
			EffectID: "late-e", ReferenceID: "late-r",
			ConfigID: plan.ConfigID, PlanID: plan.ID, Generation: plan.Generation,
			OperationKey: "ensure", EffectKey: "download", ProviderType: plan.ProviderType, ProviderDigest: plan.ProviderDigest,
		},
		RequestID: "ensure-late-e",
	}
	spec := ImmutableEnsureSpec{IdempotencyKey: "late", ArtifactID: "a", SemanticFingerprint: "fp", EnsureSpec: []byte(`{}`)}
	if d, err := reg.BeginEnsureEffect(ctx, BeginEnsureRequest{Identity: identity, Spec: spec}); err != nil || d != TransitionApplied {
		t.Fatalf("Begin: %v %v", d, err)
	}
	if _, err := reg.MarkDeleting(ctx, plan.ConfigID); err != nil {
		t.Fatal(err)
	}
	loaded, _ := store.LoadExecution(ctx, plan.ConfigID)
	var effect ActiveEffect
	for _, e := range loaded.Effects {
		if e.ID == "late-e" {
			effect = e
		}
	}
	if effect.State != ExternalEffectCancelRequested || effect.Binding != EffectBindingUnbound {
		t.Fatalf("want CancelRequested Unbound, got %#v", effect)
	}
	var ensureRetryKept bool
	for _, c := range loaded.EffectControls {
		if c.Kind == EffectControlEnsureRetry && c.State != EffectControlCompleted {
			ensureRetryKept = true
		}
	}
	if !ensureRetryKept {
		t.Fatal("EnsureRetry must be retained for late ensure")
	}
	if d, err := reg.ApplyEnsureResult(ctx, identity, EnsureEffectResult{
		EffectID: "late-e", ReferenceID: "late-r", ExternalJobID: "job-late", ExternalRevision: 1, Disposition: EnsureBound,
	}); err != nil || d != TransitionApplied {
		t.Fatalf("late Ensure: %v %v", d, err)
	}
	loaded, _ = store.LoadExecution(ctx, plan.ConfigID)
	for _, e := range loaded.Effects {
		if e.ID == "late-e" {
			if e.Binding != EffectBindingBound || e.State != ExternalEffectCancelRequested || e.ExternalJobID != "job-late" {
				t.Fatalf("late bind: %#v", e)
			}
		}
	}
	var releasePending bool
	for _, c := range loaded.EffectControls {
		if c.Kind == EffectControlRelease && c.State != EffectControlCompleted {
			releasePending = true
		}
	}
	if !releasePending {
		t.Fatal("late bind must schedule Release")
	}
}

func TestMatrixUnknownUnboundEnsureRetryAndUnknownBoundObserve(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryExecutionStore()
	reg := NewPlanRegistry(store)
	plan, _, err := reg.Install(ctx, 0, testPlan(t, "digest", model.Operation{Key: "apply", ExecutionKind: model.ExecutionDirect}))
	if err != nil {
		t.Fatal(err)
	}
	identity := TransitionIdentity{
		EffectIdentity: EffectIdentity{
			EffectID: "unk-e", ReferenceID: "unk-r",
			ConfigID: plan.ConfigID, PlanID: plan.ID, Generation: plan.Generation,
			OperationKey: "apply", EffectKey: "download", ProviderType: "test", ProviderDigest: "digest",
		},
		RequestID: "ensure-unk-e",
	}
	spec := ImmutableEnsureSpec{IdempotencyKey: "unk", ArtifactID: "a", SemanticFingerprint: "fp", EnsureSpec: []byte(`{}`)}
	if d, err := reg.BeginEnsureEffect(ctx, BeginEnsureRequest{Identity: identity, Spec: spec}); err != nil || d != TransitionApplied {
		t.Fatalf("Begin: %v %v", d, err)
	}
	if d, err := reg.ApplyEnsureResult(ctx, identity, EnsureEffectResult{
		EffectID: "unk-e", ReferenceID: "unk-r", Disposition: EnsureUnknown, Failure: EnsureFailureUnknownOutcome,
	}); err != nil || d != TransitionApplied {
		t.Fatalf("Unknown Unbound: %v %v", d, err)
	}
	loaded, _ := store.LoadExecution(ctx, plan.ConfigID)
	for _, e := range loaded.Effects {
		if e.ID == "unk-e" && (e.State != ExternalEffectUnknown || e.Binding != EffectBindingUnbound) {
			t.Fatalf("unknown unbound: %#v", e)
		}
	}
	if d, err := reg.ApplyEnsureResult(ctx, identity, EnsureEffectResult{
		EffectID: "unk-e", ReferenceID: "unk-r", ExternalJobID: "job-u", ExternalRevision: 1, Disposition: EnsureBound,
	}); err != nil || d != TransitionApplied {
		t.Fatalf("retry bind: %v %v", d, err)
	}
	observeID := ControlRequestID("observe-unk-r")
	now := time.Now()
	if _, err := reg.ClaimDueControl(ctx, plan.ConfigID, observeID, now, "att-1", "poll-1", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	obsIdentity := TransitionIdentity{EffectIdentity: identity.EffectIdentity, AttemptID: "att-1", RequestID: observeID}
	if d, err := reg.MarkEffectUnknownBound(ctx, obsIdentity, now.Add(time.Second)); err != nil || d != TransitionApplied {
		t.Fatalf("Unknown Bound: %v %v", d, err)
	}
	loaded, _ = store.LoadExecution(ctx, plan.ConfigID)
	for _, e := range loaded.Effects {
		if e.ID == "unk-e" && (e.State != ExternalEffectUnknown || e.Binding != EffectBindingBound) {
			t.Fatalf("unknown bound: %#v", e)
		}
	}
}

func TestMatrixSameRevisionDualPollYieldsBothAttempts(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryExecutionStore()
	reg := NewPlanRegistry(store)
	plan, _, err := reg.Install(ctx, 0, testPlan(t, "digest", model.Operation{Key: "apply", ExecutionKind: model.ExecutionDirect}))
	if err != nil {
		t.Fatal(err)
	}
	identity := beginBoundEffect(t, reg, plan, "dual-e", "dual-r", "job-dual")
	observeID := ControlRequestID("observe-dual-r")
	now := time.Now()
	for i, attempt := range []model.AttemptID{"att-a", "att-b"} {
		poll := PollRequestID("poll-" + string(attempt))
		if _, err := reg.ClaimDueControl(ctx, plan.ConfigID, observeID, now.Add(time.Duration(i)*time.Second), attempt, poll, now.Add(time.Minute)); err != nil {
			t.Fatalf("claim %s: %v", attempt, err)
		}
		obsIdentity := TransitionIdentity{EffectIdentity: identity.EffectIdentity, AttemptID: attempt, RequestID: observeID}
		if d, err := reg.ApplyEffectObservation(ctx, obsIdentity, EffectObservation{
			EffectID: "dual-e", AttemptID: attempt, PollRequestID: poll,
			ExternalJobID: "job-dual", ExternalRevision: 1, Disposition: DispositionStillActive,
			NextCheckAt: now,
		}); err != nil || d != TransitionApplied {
			t.Fatalf("poll %s: %v %v", attempt, d, err)
		}
	}
}

func TestMatrixWrongIdentityRejected(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryExecutionStore()
	reg := NewPlanRegistry(store)
	plan, _, err := reg.Install(ctx, 0, testPlan(t, "digest", model.Operation{Key: "apply", ExecutionKind: model.ExecutionDirect}))
	if err != nil {
		t.Fatal(err)
	}
	identity := beginBoundEffect(t, reg, plan, "id-e", "id-r", "job-id")
	observeID := ControlRequestID("observe-id-r")
	now := time.Now()
	if _, err := reg.ClaimDueControl(ctx, plan.ConfigID, observeID, now, "att-1", "poll-good", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	base := TransitionIdentity{EffectIdentity: identity.EffectIdentity, AttemptID: "att-1", RequestID: observeID}
	cases := []struct {
		name string
		mut  func(*TransitionIdentity, *EffectObservation)
	}{
		{"wrong poll", func(_ *TransitionIdentity, o *EffectObservation) { o.PollRequestID = "poll-bad" }},
		{"wrong attempt", func(id *TransitionIdentity, o *EffectObservation) { id.AttemptID = "att-bad"; o.AttemptID = "att-bad" }},
		{"wrong effect", func(id *TransitionIdentity, o *EffectObservation) {
			id.EffectIdentity.EffectID = "other"
			o.EffectID = "other"
		}},
		{"wrong reference", func(id *TransitionIdentity, _ *EffectObservation) { id.EffectIdentity.ReferenceID = "other" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id := base
			obs := EffectObservation{
				EffectID: "id-e", AttemptID: "att-1", PollRequestID: "poll-good",
				ExternalJobID: "job-id", ExternalRevision: 2, Disposition: DispositionStillActive, NextCheckAt: now.Add(time.Second),
			}
			tc.mut(&id, &obs)
			d, err := reg.ApplyEffectObservation(ctx, id, obs)
			if d == TransitionApplied && err == nil {
				t.Fatal("expected rejection")
			}
		})
	}
}

func TestMatrixStillReferencedNeverCancels(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryExecutionStore()
	reg := NewPlanRegistry(store)
	plan, _, err := reg.Install(ctx, 0, testPlan(t, "digest", model.Operation{Key: "apply", ExecutionKind: model.ExecutionDirect}))
	if err != nil {
		t.Fatal(err)
	}
	identity := beginBoundEffect(t, reg, plan, "share-e", "share-r1", "job-share")
	ref2Identity := TransitionIdentity{
		EffectIdentity: EffectIdentity{
			EffectID: "share-e", ReferenceID: "share-r2",
			ConfigID: plan.ConfigID, PlanID: plan.ID + "-g2", Generation: plan.Generation + 1,
			OperationKey: "apply", EffectKey: "download", ProviderType: "test", ProviderDigest: "digest",
		},
		RequestID: "ensure-ref-share-r2",
	}
	if d, err := reg.BeginEnsureReference(ctx, ref2Identity); err != nil || d != TransitionApplied {
		t.Fatalf("BeginEnsureReference: %v %v", d, err)
	}
	now := time.Now()
	if _, err := reg.ClaimDueControl(ctx, plan.ConfigID, ref2Identity.RequestID, now, "att-er", "poll-er", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	ref2Identity.AttemptID = "att-er"
	if d, err := reg.ApplyEnsureReferenceResult(ctx, ref2Identity, EnsureReferenceResult{
		EffectID: "share-e", ReferenceID: "share-r2", RequestID: ref2Identity.RequestID,
		ExternalJobID: "job-share", ExternalRevision: 2, Disposition: EnsureBound,
	}); err != nil || d != TransitionApplied {
		t.Fatalf("ApplyEnsureReference: %v %v", d, err)
	}

	relIdentity := TransitionIdentity{
		EffectIdentity: EffectIdentity{
			EffectID: "share-e", ReferenceID: "share-r1", ConfigID: plan.ConfigID,
			PlanID: plan.ID, Generation: plan.Generation, EffectKey: "download",
			ProviderType: "test", ProviderDigest: "digest",
		},
		RequestID: "release-share-r1",
	}
	// EnsureReference Apply already ReleaseRequested the older ref.
	claimAt := now.Add(time.Minute)
	_ = reg.WakeDueControls(ctx, claimAt)
	if _, err := reg.ClaimDueControl(ctx, plan.ConfigID, relIdentity.RequestID, claimAt, "att-rel", "poll-rel", claimAt.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	relIdentity.AttemptID = "att-rel"
	if d, err := reg.ApplyReleaseResult(ctx, relIdentity, ReleaseEffectResult{
		EffectID: "share-e", ReferenceID: "share-r1", ReleaseRequestID: relIdentity.RequestID,
		ExternalJobID: "job-share", ExternalRevision: 3, Disposition: ReleaseStillReferenced,
	}); err != nil || d != TransitionApplied {
		t.Fatalf("StillReferenced: %v %v", d, err)
	}
	loaded, _ := store.LoadExecution(ctx, plan.ConfigID)
	for _, e := range loaded.Effects {
		if e.ID == "share-e" && e.State != ExternalEffectActive {
			t.Fatalf("shared job must stay Active, got %s", e.State)
		}
	}
	_ = identity
}

func TestMatrixLastReferenceCancelObservedTerminally(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryExecutionStore()
	reg := NewPlanRegistry(store)
	plan, _, err := reg.Install(ctx, 0, testPlan(t, "digest", model.Operation{Key: "apply", ExecutionKind: model.ExecutionDirect}))
	if err != nil {
		t.Fatal(err)
	}
	identity := beginBoundEffect(t, reg, plan, "last-e", "last-r", "job-last")
	relIdentity := TransitionIdentity{
		EffectIdentity: identity.EffectIdentity, RequestID: "release-last-r",
	}
	relIdentity.EffectIdentity.ReferenceID = "last-r"
	if d, err := reg.BeginReleaseEffect(ctx, BeginReleaseRequest{Identity: relIdentity}); err != nil || d != TransitionApplied {
		t.Fatalf("BeginRelease: %v %v", d, err)
	}
	now := time.Now()
	if _, err := reg.ClaimDueControl(ctx, plan.ConfigID, relIdentity.RequestID, now, "att-rel", "poll-rel", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	relIdentity.AttemptID = "att-rel"
	if d, err := reg.ApplyReleaseResult(ctx, relIdentity, ReleaseEffectResult{
		EffectID: "last-e", ReferenceID: "last-r", ReleaseRequestID: relIdentity.RequestID,
		ExternalJobID: "job-last", ExternalRevision: 2, Disposition: ReleaseLastReferenceCancelRequested,
	}); err != nil || d != TransitionApplied {
		t.Fatalf("LastRef: %v %v", d, err)
	}
	cancelID := ControlRequestID("observe-cancel-last-e")
	claimAt := now.Add(time.Minute)
	_ = reg.WakeDueControls(ctx, claimAt)
	if _, err := reg.ClaimDueControl(ctx, plan.ConfigID, cancelID, claimAt, "att-c", "poll-c", claimAt.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	cancelIdentity := TransitionIdentity{EffectIdentity: identity.EffectIdentity, AttemptID: "att-c", RequestID: cancelID}
	if d, err := reg.ApplyEffectObservation(ctx, cancelIdentity, EffectObservation{
		EffectID: "last-e", AttemptID: "att-c", PollRequestID: "poll-c",
		ExternalJobID: "job-last", ExternalRevision: 3, Disposition: DispositionCancelled,
	}); err != nil || d != TransitionApplied {
		t.Fatalf("cancel observe: %v %v", d, err)
	}
	loaded, _ := store.LoadExecution(ctx, plan.ConfigID)
	for _, e := range loaded.Effects {
		if e.ID == "last-e" && (e.State != ExternalEffectCancelled || e.ResolutionRequired) {
			t.Fatalf("terminal cancel: %#v", e)
		}
	}
}

func TestMatrixCancelCompleteRaceAndGone(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryExecutionStore()
	reg := NewPlanRegistry(store)
	plan, _, err := reg.Install(ctx, 0, testPlan(t, "digest", model.Operation{Key: "apply", ExecutionKind: model.ExecutionDirect}))
	if err != nil {
		t.Fatal(err)
	}
	identity := beginBoundEffect(t, reg, plan, "race-e", "race-r", "job-race")
	relIdentity := TransitionIdentity{EffectIdentity: identity.EffectIdentity, RequestID: "release-race-r"}
	relIdentity.EffectIdentity.ReferenceID = "race-r"
	if d, err := reg.BeginReleaseEffect(ctx, BeginReleaseRequest{Identity: relIdentity}); err != nil || d != TransitionApplied {
		t.Fatalf("BeginRelease: %v %v", d, err)
	}
	now := time.Now()
	if _, err := reg.ClaimDueControl(ctx, plan.ConfigID, relIdentity.RequestID, now, "att-rel", "poll-rel", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	relIdentity.AttemptID = "att-rel"
	if d, err := reg.ApplyReleaseResult(ctx, relIdentity, ReleaseEffectResult{
		EffectID: "race-e", ReferenceID: "race-r", ReleaseRequestID: relIdentity.RequestID,
		ExternalJobID: "job-race", ExternalRevision: 2, Disposition: ReleaseLastReferenceCancelRequested,
	}); err != nil || d != TransitionApplied {
		t.Fatalf("LastRef: %v %v", d, err)
	}
	cancelID := ControlRequestID("observe-cancel-race-e")
	claimAt := now.Add(time.Minute)
	_ = reg.WakeDueControls(ctx, claimAt)
	if _, err := reg.ClaimDueControl(ctx, plan.ConfigID, cancelID, claimAt, "att-c", "poll-c", claimAt.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	cancelIdentity := TransitionIdentity{EffectIdentity: identity.EffectIdentity, AttemptID: "att-c", RequestID: cancelID}
	// Complete wins the cancel/complete race.
	if d, err := reg.ApplyEffectObservation(ctx, cancelIdentity, EffectObservation{
		EffectID: "race-e", AttemptID: "att-c", PollRequestID: "poll-c",
		ExternalJobID: "job-race", ExternalRevision: 3, Disposition: DispositionCompleted,
	}); err != nil || d != TransitionApplied {
		t.Fatalf("complete race: %v %v", d, err)
	}

	// Gone path on a fresh bound effect with a distinct EffectKey/slot.
	identity2 := beginBoundEffectKey(t, reg, plan, "gone-e", "gone-r", "job-gone", "download-gone")
	observeID := ControlRequestID("observe-gone-r")
	if _, err := reg.ClaimDueControl(ctx, plan.ConfigID, observeID, claimAt, "att-g", "poll-g", claimAt.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	goneIdentity := TransitionIdentity{EffectIdentity: identity2.EffectIdentity, AttemptID: "att-g", RequestID: observeID}
	if d, err := reg.ApplyEffectObservation(ctx, goneIdentity, EffectObservation{
		EffectID: "gone-e", AttemptID: "att-g", PollRequestID: "poll-g",
		ExternalJobID: "job-gone", ExternalRevision: 2, Disposition: DispositionAuthoritativeGone,
	}); err != nil || d != TransitionApplied {
		t.Fatalf("Gone: %v %v", d, err)
	}
	loaded, _ := store.LoadExecution(ctx, plan.ConfigID)
	for _, e := range loaded.Effects {
		if e.ID == "gone-e" {
			t.Fatal("Gone must remove effect")
		}
	}
}

func TestMatrixCrashRestoreEffectControlStates(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryExecutionStore()
	reg := NewPlanRegistry(store)
	plan, _, err := reg.Install(ctx, 0, testPlan(t, "digest", model.Operation{Key: "apply", ExecutionKind: model.ExecutionDirect}))
	if err != nil {
		t.Fatal(err)
	}
	_ = beginBoundEffect(t, reg, plan, "crash-e", "crash-r", "job-crash")
	if _, err := reg.MarkDeleting(ctx, plan.ConfigID); err != nil {
		t.Fatal(err)
	}
	recovered := NewPlanRegistry(store)
	if err := recovered.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if !recovered.IsDeleting(plan.ConfigID) {
		t.Fatal("deleting tombstone lost")
	}
	if recovered.DeletionReady(plan.ConfigID) {
		t.Fatal("must stay fail-closed until releases finish")
	}
}

func TestMatrixAdministratorResolveFailedEffect(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryExecutionStore()
	reg := NewPlanRegistry(store)
	plan, _, err := reg.Install(ctx, 0, testPlan(t, "digest", model.Operation{Key: "apply", ExecutionKind: model.ExecutionDirect}))
	if err != nil {
		t.Fatal(err)
	}
	identity := TransitionIdentity{
		EffectIdentity: EffectIdentity{
			EffectID: "fail-e", ReferenceID: "fail-r",
			ConfigID: plan.ConfigID, PlanID: plan.ID, Generation: plan.Generation,
			OperationKey: "apply", EffectKey: "download", ProviderType: "test", ProviderDigest: "digest",
		},
		RequestID: "ensure-fail-e",
	}
	spec := ImmutableEnsureSpec{IdempotencyKey: "fail", ArtifactID: "a", SemanticFingerprint: "fp", EnsureSpec: []byte(`{}`)}
	if d, err := reg.BeginEnsureEffect(ctx, BeginEnsureRequest{Identity: identity, Spec: spec}); err != nil || d != TransitionApplied {
		t.Fatalf("Begin: %v %v", d, err)
	}
	if d, err := reg.ApplyEnsureResult(ctx, identity, EnsureEffectResult{
		EffectID: "fail-e", ReferenceID: "fail-r", Disposition: EnsureFailed, Failure: EnsureFailureUnknownOutcome,
	}); err != nil || d != TransitionApplied {
		t.Fatalf("Failed: %v %v", d, err)
	}
	if _, err := reg.MarkDeleting(ctx, plan.ConfigID); err != nil {
		t.Fatal(err)
	}
	if reg.DeletionReady(plan.ConfigID) {
		t.Fatal("Failed ResolutionRequired must block deletion")
	}
	if d, err := reg.AdministratorResolveFailedEffect(ctx, plan.ConfigID, "fail-e", "ops ticket 1"); err != nil || d != TransitionApplied {
		t.Fatalf("admin resolve: %v %v", d, err)
	}
	if !reg.DeletionReady(plan.ConfigID) {
		t.Fatal("after admin resolve deletion should be ready")
	}
}

func TestMatrixServiceOutageMarksUnknownBound(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryExecutionStore()
	reg := NewPlanRegistry(store)
	plan, _, err := reg.Install(ctx, 0, testPlan(t, "digest", model.Operation{Key: "apply", ExecutionKind: model.ExecutionDirect}))
	if err != nil {
		t.Fatal(err)
	}
	identity := beginBoundEffect(t, reg, plan, "outage-e", "outage-r", "job-outage")
	observeID := ControlRequestID("observe-outage-r")
	now := time.Now()
	if _, err := reg.ClaimDueControl(ctx, plan.ConfigID, observeID, now, "att-1", "poll-1", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	obsIdentity := TransitionIdentity{EffectIdentity: identity.EffectIdentity, AttemptID: "att-1", RequestID: observeID}
	if d, err := reg.MarkEffectUnknownBound(ctx, obsIdentity, now.Add(time.Second)); err != nil || d != TransitionApplied {
		t.Fatalf("outage: %v %v", d, err)
	}
	loaded, _ := store.LoadExecution(ctx, plan.ConfigID)
	for _, e := range loaded.Effects {
		if e.ID == "outage-e" && e.State != ExternalEffectUnknown {
			t.Fatalf("want Unknown Bound, got %#v", e)
		}
	}
	for _, c := range loaded.EffectControls {
		if c.ID == observeID && c.State != EffectControlYielded {
			t.Fatalf("observe should yield for retry, got %s", c.State)
		}
	}
}
