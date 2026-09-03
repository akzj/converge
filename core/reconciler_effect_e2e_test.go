package core

import (
	"context"
	"testing"
	"time"

	"github.com/akzj/converge/pkg/model"
)

func TestEffectEnsureThenObserveThenCompletedE2E(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	service := NewFakeDownloadService()
	provider := NewFakeDownloadProvider("sha256:mock-fake", service)
	store := NewMemoryStateStore()
	execStore := NewMemoryExecutionStore()

	r := NewReconciler(store, execStore, NewMemoryEventBus(), NewMemoryArbiter(), NewMemoryJournal())
	r.RegisterProvider(ctx, provider)

	go func() { _ = r.Run(ctx) }()

	desired := model.DesiredState{
		ConfigID:     model.ConfigID{Name: "effect-config"},
		ProviderType: "fake_download",
		Version:      1,
		Digest:       "sha256:63ab5330cde2137ab0241ffc63eaae1b221e1dcd8ebc49f6417f3d7ac6eaf1f5",
		Spec:         []byte(`{"url":"https://example.com/file.bin"}`),
	}

	if err := r.SubmitDesired(ctx, desired); err != nil {
		t.Fatal(err)
	}

	// Wait for the ensure operation to be called.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if provider.Counts().EnsureCount > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if provider.Counts().EnsureCount == 0 {
		t.Fatal("ensure was never called")
	}
	t.Logf("ensure called %d times", provider.ensureCount)

	// Durable binding must exist after ensure.
	deadline = time.Now().Add(2 * time.Second)
	var bound bool
	for time.Now().Before(deadline) {
		plans := r.registry.ExecutionPlans()
		for _, plan := range plans {
			if plan.ConfigID.Name != desired.ConfigID.Name {
				continue
			}
			if effect, _, ok := r.registry.LookupEffectBinding(plan.ConfigID, plan.ID, plan.Generation, "download"); ok && effect.ExternalJobID != "" && effect.Binding == EffectBindingBound {
				bound = true
				break
			}
		}
		if bound {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !bound {
		t.Fatal("ensure did not persist a bound ActiveEffect")
	}

	// Wait for the observe operation to start and return Waiting.
	for time.Now().Before(deadline) {
		if provider.Counts().ObserveCount > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if provider.Counts().ObserveCount == 0 {
		t.Fatal("observe was never called")
	}
	initialObserveCount := provider.Counts().ObserveCount
	t.Logf("observe initially called %d times", initialObserveCount)

	// Check that the config is still converging (ensure done, observe waiting).
	r.mu.RLock()
	status := r.configs[desired.ConfigID.Name].Status
	r.mu.RUnlock()
	if status != model.ConfigConverging {
		t.Fatalf("status=%s, want converging", status)
	}

	// Advance the download job to ready.
	job := "download-1"
	if err := service.AdvanceJob(job, FakeJobDownloading); err != nil {
		t.Fatal(err)
	}
	if err := service.AdvanceJob(job, FakeJobVerifying); err != nil {
		t.Fatal(err)
	}
	if err := service.AdvanceJob(job, FakeJobReady); err != nil {
		t.Fatal(err)
	}
	t.Log("job advanced to ready")

	// Drive control scheduler and waiting wakeups until observe completes.
	for time.Now().Before(deadline) {
		_ = r.registry.WakeDueWaiting(ctx, time.Now().Add(10*time.Second))
		_ = r.registry.WakeDueControls(ctx, time.Now())
		r.processDueControls(ctx)
		r.executeReady(ctx)
		r.mu.RLock()
		config := r.configs[desired.ConfigID.Name]
		converged := config != nil && config.Status == model.ConfigConverged
		r.mu.RUnlock()
		if converged {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	r.mu.RLock()
	finalStatus := r.configs[desired.ConfigID.Name].Status
	r.mu.RUnlock()
	if finalStatus != model.ConfigConverged {
		snap := r.registry.Snapshot(desired.ConfigID)
		if snap.Plan != nil {
			for key, node := range snap.Plan.Nodes {
				t.Logf("node %s status=%s kind=%s", key, node.Status, node.Operation.ExecutionKind)
			}
		}
		t.Fatalf("final status=%s, want converged", finalStatus)
	}

	t.Logf("ensure called %d times, observe called %d times, config converged", provider.Counts().EnsureCount, provider.Counts().ObserveCount)
}

func TestEffectSupersessionSameArtifactReuses(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	service := NewFakeDownloadService()
	provider := NewFakeDownloadProvider("sha256:mock-fake", service)
	r := NewReconciler(NewMemoryStateStore(), NewMemoryExecutionStore(), NewMemoryEventBus(), NewMemoryArbiter(), NewMemoryJournal())
	r.RegisterProvider(ctx, provider)
	go func() { _ = r.Run(ctx) }()

	v1 := model.DesiredState{
		ConfigID: model.ConfigID{Name: "supersede-config"}, ProviderType: "fake_download",
		Version: 1, Digest: "sha256:2430f1a2ad2982d0067885488a4c89e21ad1d7c83b115ba8f1b20acc88dfaea8", Spec: []byte(`{"version":1}`),
	}
	if err := r.SubmitDesired(ctx, v1); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if provider.Counts().EnsureCount >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	firstJob := provider.LastJobID
	if firstJob == "" {
		t.Fatal("expected job after ensure")
	}

	if err := r.SubmitDesired(ctx, model.DesiredState{
		ConfigID: model.ConfigID{Name: "supersede-config"}, ProviderType: "fake_download",
		Version: 2, Digest: "sha256:2430f1a2ad2982d0067885488a4c89e21ad1d7c83b115ba8f1b20acc88dfaea8", Spec: []byte(`{"version":1}`),
	}); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_ = r.registry.WakeDueControls(ctx, time.Now())
		r.processDueControls(ctx)
		r.executeReady(ctx)
		time.Sleep(10 * time.Millisecond)
		if provider.Counts().EnsureCount >= 1 {
			// Wait until ensure-ref has a chance to run; ensure count must stay at 1.
			if !r.registry.HasActiveEffectControl(model.ConfigID{Name: "supersede-config"}, "download", EffectControlEnsureReference) {
				break
			}
		}
	}
	if provider.Counts().EnsureCount != 1 {
		t.Fatalf("same artifact must not CreateOrGet again: ensure=%d", provider.Counts().EnsureCount)
	}
	if provider.LastJobID != firstJob {
		t.Fatalf("same artifact should reuse job: got %q want %q", provider.LastJobID, firstJob)
	}
	t.Logf("ensure=%d observe=%d release=%d job=%s", provider.Counts().EnsureCount, provider.Counts().ObserveCount, provider.Counts().ReleaseCount, provider.LastJobID)
}

func TestEffectSupersessionDifferentArtifact(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	service := NewFakeDownloadService()
	provider := NewFakeDownloadProvider("sha256:mock-fake", service)
	r := NewReconciler(NewMemoryStateStore(), NewMemoryExecutionStore(), NewMemoryEventBus(), NewMemoryArbiter(), NewMemoryJournal())
	r.RegisterProvider(ctx, provider)
	go func() { _ = r.Run(ctx) }()

	if err := r.SubmitDesired(ctx, model.DesiredState{
		ConfigID: model.ConfigID{Name: "diff-config"}, ProviderType: "fake_download",
		Version: 1, Digest: "sha256:afbf9d0f3560b0fd7795e81c42a0a79ee6b6fc67e064f77826aee642cad28d91", Spec: []byte(`{"v":1}`),
	}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if provider.Counts().EnsureCount >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	firstJob := provider.LastJobID

	if err := r.SubmitDesired(ctx, model.DesiredState{
		ConfigID: model.ConfigID{Name: "diff-config"}, ProviderType: "fake_download",
		Version: 2, Digest: "sha256:2b5442799fccc3af2e7e790017697373913b7afcac933d72fb5876de994f659a", Spec: []byte(`{"v":2}`),
	}); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_ = r.registry.WakeDueControls(ctx, time.Now())
		r.processDueControls(ctx)
		r.executeReady(ctx)
		if firstJob != "" {
			if state, _, _, err := service.GetJob(firstJob); err == nil && state == FakeJobCancelling {
				_ = service.AdvanceJob(firstJob, FakeJobCancelled)
			}
		}
		if provider.Counts().EnsureCount >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if provider.Counts().EnsureCount < 2 {
		t.Fatalf("changed artifact should ensure again, ensure=%d", provider.Counts().EnsureCount)
	}
	if provider.LastJobID == "" || provider.LastJobID == firstJob {
		t.Fatalf("changed artifact should bind a new job, first=%q last=%q", firstJob, provider.LastJobID)
	}
	hasRelease := r.registry.HasActiveEffectControl(model.ConfigID{Name: "diff-config"}, "download", EffectControlRelease) ||
		provider.Counts().ReleaseCount > 0
	if !hasRelease {
		t.Fatal("changed artifact should schedule release of old reference")
	}
	t.Logf("ensure=%d observe=%d release=%d", provider.Counts().EnsureCount, provider.Counts().ObserveCount, provider.Counts().ReleaseCount)
}

func TestEffectObserveControlReclaimAfterLeaseExpiry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	service := NewFakeDownloadService()
	provider := NewFakeDownloadProvider("sha256:mock-fake", service)
	store := NewMemoryExecutionStore()
	r := NewReconciler(NewMemoryStateStore(), store, NewMemoryEventBus(), NewMemoryArbiter(), NewMemoryJournal())
	r.RegisterProvider(ctx, provider)
	go func() { _ = r.Run(ctx) }()

	desired := model.DesiredState{
		ConfigID: model.ConfigID{Name: "reclaim-e2e"}, ProviderType: "fake_download",
		Version: 1, Digest: "sha256:c444e7e4c3ecef19664501ae12d3e63ccb16be4b2b241f349961c04a9951082b", Spec: []byte(`{"url":"x"}`),
	}
	if err := r.SubmitDesired(ctx, desired); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if provider.Counts().EnsureCount >= 1 && provider.LastJobID != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if provider.LastJobID == "" {
		t.Fatal("expected ensure binding")
	}
	observeBefore := provider.Counts().ObserveCount
	// Force reclaim by expiring any in-flight observe leases, then resume.
	r.registry.ReclaimExpiredControls(ctx, time.Now().Add(time.Hour))
	_ = r.registry.WakeDueControls(ctx, time.Now())
	r.processDueControls(ctx)
	for time.Now().Before(deadline) {
		_ = r.registry.WakeDueControls(ctx, time.Now())
		r.processDueControls(ctx)
		r.executeReady(ctx)
		if provider.Counts().ObserveCount > observeBefore {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if provider.Counts().ObserveCount == 0 {
		t.Fatal("observe control should run after reclaim/wake")
	}
}
