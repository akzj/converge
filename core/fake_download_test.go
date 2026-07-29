package core

import (
	"context"
	"testing"

	"github.com/akzj/converge/pkg/model"
)

func newTestEnsureRequest(idemKey string, refID ReferenceID) EnsureEffectRequest {
	return EnsureEffectRequest{
		Identity:            EffectIdentity{EffectID: "effect", ReferenceID: refID, ConfigID: model.ConfigID{Name: "config"}, PlanID: "plan", Generation: 1, OperationKey: "ensure", EffectKey: "download", ProviderDigest: "v1"},
		IdempotencyKey:      idemKey,
		ArtifactID:          "sha256:artifact",
		SemanticFingerprint: "fp",
		EnsureSpec:          []byte(`{"url":"https://example.com/artifact"}`),
	}
}

func newTestObserveRequest(attemptID model.AttemptID, pollID PollRequestID, jobID string) []ObserveEffectRequest {
	return []ObserveEffectRequest{{Identity: EffectIdentity{EffectID: "effect", ReferenceID: "ref", EffectKey: "download"}, AttemptID: attemptID, PollRequestID: pollID, ExternalJobID: jobID, ExternalRevision: 1}}
}

func TestFakeDownloadEnsureIdempotency(t *testing.T) {
	service := NewFakeDownloadService()
	provider := NewFakeDownloadProvider("v1", service)

	first, err := provider.EnsureEffect(context.Background(), newTestEnsureRequest("idem-key", "ref"))
	if err != nil {
		t.Fatal(err)
	}
	if first.ExternalJobID == "" || first.Disposition != EnsureBound {
		t.Fatalf("first ensure: %#v", first)
	}
	if provider.ensureCount != 1 {
		t.Fatalf("ensure count=%d", provider.ensureCount)
	}

	second, err := provider.EnsureEffect(context.Background(), newTestEnsureRequest("idem-key", "ref"))
	if err != nil {
		t.Fatal(err)
	}
	if second.ExternalJobID != first.ExternalJobID {
		t.Fatalf("different jobs: %q vs %q", first.ExternalJobID, second.ExternalJobID)
	}
	if provider.ensureCount != 2 {
		t.Fatalf("ensure count=%d", provider.ensureCount)
	}
}

func TestFakeDownloadObserveStateMapping(t *testing.T) {
	service := NewFakeDownloadService()
	provider := NewFakeDownloadProvider("v1", service)

	ensureResult, err := provider.EnsureEffect(context.Background(), newTestEnsureRequest("observe-mapping", "ref"))
	if err != nil {
		t.Fatal(err)
	}

	// Job starts queued → should be StillActive
	observe1, err := provider.ObserveEffects(context.Background(), newTestObserveRequest("attempt-1", "poll-1", ensureResult.ExternalJobID))
	if err != nil {
		t.Fatal(err)
	}
	if observe1["poll-1"].Observation == nil || observe1["poll-1"].Observation.Disposition != DispositionStillActive {
		t.Fatalf("queued observation: %#v", observe1)
	}

	// Advance to ready
	if err := service.AdvanceJob(ensureResult.ExternalJobID, FakeJobDownloading); err != nil {
		t.Fatal(err)
	}
	if err := service.AdvanceJob(ensureResult.ExternalJobID, FakeJobVerifying); err != nil {
		t.Fatal(err)
	}
	if err := service.AdvanceJob(ensureResult.ExternalJobID, FakeJobReady); err != nil {
		t.Fatal(err)
	}

	observe2, err := provider.ObserveEffects(context.Background(), newTestObserveRequest("attempt-2", "poll-2", ensureResult.ExternalJobID))
	if err != nil {
		t.Fatal(err)
	}
	if observe2["poll-2"].Observation.Disposition != DispositionCompleted {
		t.Fatalf("ready observation: %#v", observe2)
	}
}

func TestFakeDownloadJobNotFoundIsRetryable(t *testing.T) {
	provider := NewFakeDownloadProvider("v1", NewFakeDownloadService())
	result, err := provider.ObserveEffects(context.Background(), newTestObserveRequest("attempt", "poll", "nonexistent"))
	if err != nil {
		t.Fatal(err)
	}
	if result["poll"].Error == nil || !result["poll"].Error.Retryable {
		t.Fatalf("not found error: %#v", result)
	}
}

func TestFakeDownloadEnsureReference(t *testing.T) {
	service := NewFakeDownloadService()
	provider := NewFakeDownloadProvider("v1", service)

	ensureResult, err := provider.EnsureEffect(context.Background(), newTestEnsureRequest("reference-test", "old-ref"))
	if err != nil {
		t.Fatal(err)
	}

	refResult, err := provider.EnsureReference(context.Background(), EnsureReferenceRequest{
		Identity:  EffectIdentity{EffectID: "effect", ReferenceID: "new-ref", EffectKey: "download"},
		RequestID: "control-1", ExternalJobID: ensureResult.ExternalJobID, ExternalRevision: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if refResult.Disposition != EnsureBound {
		t.Fatalf("ensure reference: %#v", refResult)
	}

	// Both references should be active.
	_, _, refs, err := service.GetJob(ensureResult.ExternalJobID)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]bool, len(refs))
	for _, ref := range refs {
		seen[ref] = true
	}
	if !seen["old-ref"] || !seen["new-ref"] {
		t.Fatalf("missing references: %#v", refs)
	}
}

func TestFakeDownloadLastReferenceCancellation(t *testing.T) {
	service := NewFakeDownloadService()
	provider := NewFakeDownloadProvider("v1", service)

	ensureResult, err := provider.EnsureEffect(context.Background(), newTestEnsureRequest("last-ref", "ref"))
	if err != nil {
		t.Fatal(err)
	}

	// Release the only reference → should trigger last-reference cancellation.
	release, err := provider.ReleaseEffect(context.Background(), ReleaseEffectRequest{
		Identity:         EffectIdentity{EffectID: "effect", ReferenceID: "ref", EffectKey: "download"},
		ReleaseRequestID: "release-ref", ExternalJobID: ensureResult.ExternalJobID, ExternalRevision: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if release.Disposition != ReleaseLastReferenceCancelRequested {
		t.Fatalf("expected last-reference cancellation: %#v", release)
	}

	// Removing same reference again should be ReleaseConfirmed (idempotent).
	release2, err := provider.ReleaseEffect(context.Background(), ReleaseEffectRequest{
		Identity:         EffectIdentity{EffectID: "effect", ReferenceID: "ref", EffectKey: "download"},
		ReleaseRequestID: "release-again", ExternalJobID: ensureResult.ExternalJobID, ExternalRevision: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if release2.Disposition != ReleaseConfirmed {
		t.Fatalf("idempotent release: %#v", release2)
	}

	// Job should be in cancelling state.
	state, _, _, _ := service.GetJob(ensureResult.ExternalJobID)
	if state != FakeJobCancelling {
		t.Fatalf("state=%s, want cancelling", state)
	}
}

func TestFakeDownloadBatchObservationCardinality(t *testing.T) {
	service := NewFakeDownloadService()
	provider := NewFakeDownloadProvider("v1", service)

	result1, _ := provider.EnsureEffect(context.Background(), newTestEnsureRequest("batch-a", "ref-a"))
	result2, _ := provider.EnsureEffect(context.Background(), newTestEnsureRequest("batch-b", "ref-b"))

	// Observe both in one batch.
	requests := []ObserveEffectRequest{
		{Identity: EffectIdentity{EffectID: "effect", ReferenceID: "ref-a", EffectKey: "download"}, AttemptID: "att-1", PollRequestID: "poll-a", ExternalJobID: result1.ExternalJobID, ExternalRevision: 1},
		{Identity: EffectIdentity{EffectID: "effect", ReferenceID: "ref-b", EffectKey: "download"}, AttemptID: "att-2", PollRequestID: "poll-b", ExternalJobID: result2.ExternalJobID, ExternalRevision: 1},
	}
	observations, err := provider.ObserveEffects(context.Background(), requests)
	if err != nil {
		t.Fatal(err)
	}
	if len(observations) != 2 {
		t.Fatalf("observations=%d", len(observations))
	}
	if observations["poll-a"].Observation == nil || observations["poll-b"].Observation == nil {
		t.Fatalf("missing observation: %#v", observations)
	}
}

func TestOwnObservationRefCrossesEffectBarrier(t *testing.T) {
	effect := ActiveEffect{
		ID: "effect", Binding: EffectBindingBound, ExternalJobID: "job", ArtifactID: "sha256:x",
		IdempotencyKey: "idem", SemanticFingerprint: "fp", ProviderType: "download", ProviderDigest: "v1",
		ConflictKey: "artifact/x", State: ExternalEffectActive, ResolutionRequired: true, ExternalRevision: 1,
	}
	plan := &model.Plan{ID: "plan", ConfigID: model.ConfigID{Name: "config"}, Generation: 1}
	reference := EffectReference{ID: "ref", EffectID: effect.ID, ConfigID: plan.ConfigID, PlanID: plan.ID, Generation: plan.Generation, EffectKey: "download", State: EffectReferenceActive}

	// Effect control operations should NOT be blocked by own barrier.
	for _, kind := range []model.OperationExecutionKind{model.ExecutionEffectEnsure, model.ExecutionEffectObserve, model.ExecutionEffectRelease} {
		op := model.Operation{ExecutionKind: kind, EffectKey: "download", ConflictKey: effect.ConflictKey, ReleaseTarget: model.ReleaseCurrentPlan}
		if OperationBlockedByEffect(op, plan, effect, reference) {
			t.Fatalf("control operation %q was blocked by own effect barrier", kind)
		}
	}

	// Direct operation with same conflict should be blocked.
	direct := model.Operation{ExecutionKind: model.ExecutionDirect, ConflictKey: effect.ConflictKey}
	if !OperationBlockedByEffect(direct, plan, effect, reference) {
		t.Fatal("direct operation was not blocked")
	}
}
