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
		Digest:       "v1",
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

	// Wake up waiting controls so the next observe picks up the ready state.
	// Use a future time to ensure NextCheckAt has passed.
	if err := r.registry.WakeDueWaiting(ctx, time.Now().Add(10*time.Second)); err != nil {
		t.Fatal(err)
	}
	r.executeReady(ctx)

	// Wait for the config to converge.
	for time.Now().Before(deadline) {
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
		t.Fatalf("final status=%s, want converged", finalStatus)
	}

	t.Logf("ensure called %d times, observe called %d times, config converged", provider.Counts().EnsureCount, provider.Counts().ObserveCount)
}
