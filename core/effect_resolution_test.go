package core

import (
	"context"
	"testing"

	"github.com/akzj/converge/pkg/model"
)

func TestResolveUnknownEffectsReleasesBarrier(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryExecutionStore()
	first := NewPlanRegistry(store)
	installed, _, err := first.Install(ctx, 0, testPlan(t, "digest", model.Operation{Key: "apply", ConflictKey: "resource/x"}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.StartAttempt(ctx, installed.ConfigID, installed.Generation, "apply", "attempt-1"); err != nil {
		t.Fatal(err)
	}

	recovered := NewPlanRegistry(store)
	if err := recovered.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ready := recovered.ReadyOperations(installed.ConfigID); len(ready) != 0 {
		t.Fatalf("unknown effect did not block: %#v", ready)
	}
	if err := recovered.ResolveEffects(ctx, installed.ConfigID, map[model.AttemptID]EffectResolution{"attempt-1": EffectAbsent}); err != nil {
		t.Fatal(err)
	}
	if _, ready := recovered.ReadyOperations(installed.ConfigID); len(ready) != 1 || ready[0].Key != "apply" {
		t.Fatalf("absent resolution did not release/requeue: %#v", ready)
	}

	restarted := NewPlanRegistry(store)
	if err := restarted.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ready := restarted.ReadyOperations(installed.ConfigID); len(ready) != 1 {
		t.Fatalf("resolution was not durable: %#v", ready)
	}
}

func TestResolveUnknownEffectCompletedMarksNodeComplete(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryExecutionStore()
	first := NewPlanRegistry(store)
	installed, _, err := first.Install(ctx, 0, testPlan(t, "digest", model.Operation{Key: "apply"}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.StartAttempt(ctx, installed.ConfigID, installed.Generation, "apply", "attempt-1"); err != nil {
		t.Fatal(err)
	}
	recovered := NewPlanRegistry(store)
	if err := recovered.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if err := recovered.ResolveEffects(ctx, installed.ConfigID, map[model.AttemptID]EffectResolution{"attempt-1": EffectCompleted}); err != nil {
		t.Fatal(err)
	}
	if status := recovered.Snapshot(installed.ConfigID).Plan.Nodes["apply"].Status; status != model.NodeCompleted {
		t.Fatalf("status=%s", status)
	}
}
