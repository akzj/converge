package core

import (
	"context"
	"testing"

	"github.com/akzj/converge/pkg/model"
)

func TestMarkDeletingStopsSchedulingAndPersistsTombstone(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryExecutionStore()
	registry := NewPlanRegistry(store)
	installed, _, err := registry.Install(ctx, 0, testPlan(t, "digest", model.Operation{Key: "running", CancelMode: model.CancelModeSafe}, model.Operation{Key: "pending"}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.StartAttempt(ctx, installed.ConfigID, installed.Generation, "running", "attempt"); err != nil {
		t.Fatal(err)
	}
	attempts, err := registry.MarkDeleting(ctx, installed.ConfigID)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 1 || attempts[0].Status != model.AttemptCancelling {
		t.Fatalf("deletion attempts=%#v", attempts)
	}
	if !registry.IsDeleting(installed.ConfigID) {
		t.Fatal("deleting tombstone not visible")
	}
	if plan, ready := registry.ReadyOperations(installed.ConfigID); plan != nil || len(ready) != 0 {
		t.Fatalf("deleting config remained schedulable: %#v %#v", plan, ready)
	}

	recovered := NewPlanRegistry(store)
	if err := recovered.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if !recovered.IsDeleting(installed.ConfigID) {
		t.Fatal("deleting tombstone not durable")
	}
	if plan, ready := recovered.ReadyOperations(installed.ConfigID); plan != nil || len(ready) != 0 {
		t.Fatalf("recovered deleting config schedulable: %#v %#v", plan, ready)
	}
}

func TestReconcilerDeletionCascadesAndFinalizes(t *testing.T) {
	ctx := context.Background()
	stateStore := NewMemoryStateStore()
	executionStore := NewMemoryExecutionStore()
	r := NewReconciler(stateStore, executionStore, NewMemoryEventBus(), NewMemoryArbiter(), NewMemoryJournal())
	configs := []struct {
		name    string
		depends []string
	}{{"upstream", nil}, {"downstream", []string{"upstream"}}}
	for _, config := range configs {
		desired := model.DesiredState{ConfigID: model.ConfigID{Name: config.name}, ProviderType: "test", Version: 1, Digest: config.name, DependsOn: config.depends}
		candidate, err := BuildCandidate(desired.ConfigID, desired, "test", "digest", nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := r.registry.Install(ctx, 0, candidate); err != nil {
			t.Fatal(err)
		}
		recorded := model.RecordedState{ConfigID: desired.ConfigID, ProviderType: "test", DesiredVersion: 1, DesiredDigest: config.name, Status: model.ConfigConverged}
		if err := stateStore.Record(ctx, recorded); err != nil {
			t.Fatal(err)
		}
		r.configs[config.name] = &model.ManagedConfig{ID: desired.ConfigID, Desired: desired, Recorded: recorded, DependsOnConfigs: config.depends, Status: model.ConfigConverged}
	}
	r.deleteConfig(ctx, "upstream")
	if len(r.configs) != 0 {
		t.Fatalf("configs remain after cascade: %#v", r.configs)
	}
	for _, name := range []string{"upstream", "downstream"} {
		id := model.ConfigID{Name: name}
		if snapshot := r.registry.Snapshot(id); snapshot.Plan != nil {
			t.Fatalf("execution remains for %s", name)
		}
		if recorded, err := stateStore.Get(ctx, id); err != nil || recorded != nil {
			t.Fatalf("record remains for %s: %#v err=%v", name, recorded, err)
		}
	}
}
