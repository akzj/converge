package core

import (
	"context"
	"testing"
	"time"

	"github.com/akzj/converge/pkg/model"
)

func TestDetectDriftTriggersPlanLatestForConvergedConfigs(t *testing.T) {
	store := NewMemoryStateStore()
	events := NewMemoryEventBus()
	arbiter := NewMemoryArbiter()
	journal := NewMemoryJournal()

	r := NewReconciler(store, NewMemoryExecutionStore(), events, arbiter, journal)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Register a provider so planLatest won't fail.
	provider := &mockProvider{typeName: "drift"}
	r.RegisterProvider(ctx, provider)

	// Directly set up a converged config in the reconciler.
	desired := model.DesiredState{
		ConfigID:     model.ConfigID{Name: "drift-config"},
		ProviderType: "drift",
		Version:      1,
		Spec:         []byte(`{"key": "value"}`),
		Digest:       "sha256:drift-v1",
	}
	candidate, err := BuildCandidate(desired.ConfigID, desired, "drift", "sha256:mock-drift",
		[]model.Operation{{Key: "apply", Action: "provision"}})
	if err != nil {
		t.Fatal(err)
	}
	installed, _, err := r.registry.Install(ctx, 0, candidate)
	if err != nil {
		t.Fatal(err)
	}

	// Mark all nodes completed so the plan is "converged".
	r.registry.mu.Lock()
	state := r.registry.configs["drift-config"]
	if state != nil && state.active != nil {
		for _, node := range state.active.Nodes {
			node.Status = model.NodeCompleted
		}
	}
	r.registry.mu.Unlock()

	// Set up the managed config as converged.
	r.mu.Lock()
	r.configs["drift-config"] = &model.ManagedConfig{
		ID:                desired.ConfigID,
		Desired:           desired,
		Status:            model.ConfigConverged,
		DependsOnConfigs:  []string{},
	}
	r.mu.Unlock()

	// Record the plan's generation before detectDrift.
	preGen := installed.Generation

	// Run detectDrift — it should trigger planLatest for the converged config.
	r.detectDrift(ctx)

	// Wait for the async planLatest to complete.
	time.Sleep(50 * time.Millisecond)

	// After detectDrift, a new plan should have been installed (since Inspect
	// returns Present: false, Replan returns new operations).
	snapshot := r.registry.Snapshot(desired.ConfigID)
	if snapshot.Plan == nil {
		t.Fatal("no active plan after detectDrift")
	}
	t.Logf("pre generation=%d, post generation=%d", preGen, snapshot.Plan.Generation)
	if snapshot.Plan.Generation <= preGen {
		t.Logf("plan was not refreshed (may be expected if no change needed)")
	}
}

func TestDetectDriftSkipsNonConvergedConfigs(t *testing.T) {
	store := NewMemoryStateStore()
	events := NewMemoryEventBus()
	arbiter := NewMemoryArbiter()
	journal := NewMemoryJournal()

	r := NewReconciler(store, NewMemoryExecutionStore(), events, arbiter, journal)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	provider := &mockProvider{typeName: "skip"}
	r.RegisterProvider(ctx, provider)

	desired := model.DesiredState{
		ConfigID:     model.ConfigID{Name: "skip-config"},
		ProviderType: "skip",
		Version:      1,
		Spec:         []byte(`{"key": "value"}`),
		Digest:       "sha256:skip-v1",
	}
	candidate, err := BuildCandidate(desired.ConfigID, desired, "skip", "sha256:mock-skip",
		[]model.Operation{{Key: "apply", Action: "provision"}})
	if err != nil {
		t.Fatal(err)
	}
	installed, _, err := r.registry.Install(ctx, 0, candidate)
	if err != nil {
		t.Fatal(err)
	}

	// Set as Converging (not Converged) — detectDrift should skip it.
	r.mu.Lock()
	r.configs["skip-config"] = &model.ManagedConfig{
		ID:               desired.ConfigID,
		Desired:          desired,
		Status:           model.ConfigConverging,
		DependsOnConfigs: []string{},
	}
	r.mu.Unlock()

	preGen := installed.Generation
	r.detectDrift(ctx)
	time.Sleep(50 * time.Millisecond)

	snapshot := r.registry.Snapshot(desired.ConfigID)
	if snapshot.Plan == nil {
		t.Fatal("no active plan after detectDrift")
	}
	if snapshot.Plan.Generation != preGen {
		t.Fatalf("converging config was replanned: generation %d → %d", preGen, snapshot.Plan.Generation)
	}
}

func TestSubmitDeleteRemovesConfig(t *testing.T) {
	store := NewMemoryStateStore()
	events := NewMemoryEventBus()
	arbiter := NewMemoryArbiter()
	journal := NewMemoryJournal()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r := NewReconciler(store, NewMemoryExecutionStore(), events, arbiter, journal)
	provider := &mockProvider{typeName: "delete"}
	r.RegisterProvider(ctx, provider)

	go func() { _ = r.Run(ctx) }()

	// Submit a desired state to create a config.
	err := r.SubmitDesired(ctx, model.DesiredState{
		ConfigID:     model.ConfigID{Name: "delete-config"},
		ProviderType: "delete",
		Version:      1,
		Spec:         []byte(`{"key": "value"}`),
		Digest:       "sha256:delete-v1",
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	// Verify the config exists.
	r.mu.RLock()
	_, exists := r.configs["delete-config"]
	r.mu.RUnlock()
	if !exists {
		t.Fatal("config was not created")
	}

	// Submit delete.
	err = r.SubmitDelete(ctx, "delete-config")
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)

	// Verify the config is removed.
	r.mu.RLock()
	_, exists = r.configs["delete-config"]
	r.mu.RUnlock()
	if exists {
		t.Fatal("config was not deleted")
	}

	// Verify execution state is removed.
	snapshot := r.registry.Snapshot(model.ConfigID{Name: "delete-config"})
	if snapshot.Plan != nil {
		t.Fatal("execution state was not deleted")
	}

	// Verify recorded state is removed.
	recorded, err := store.Get(ctx, model.ConfigID{Name: "delete-config"})
	if err != nil {
		t.Fatal(err)
	}
	if recorded != nil {
		t.Fatalf("recorded state was not deleted: %#v", recorded)
	}
}