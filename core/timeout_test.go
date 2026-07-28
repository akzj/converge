package core

import (
	"context"
	"testing"

	"github.com/akzj/converge/pkg/model"
)

func TestMarkTimedOutAttemptUnknownKeepsConflictBarrier(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryExecutionStore()
	registry := NewPlanRegistry(store)
	installed, _, err := registry.Install(ctx, 0, testPlan(t, "digest", model.Operation{Key: "apply", ConflictKey: "resource/x"}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.StartAttempt(ctx, installed.ConfigID, installed.Generation, "apply", "attempt"); err != nil {
		t.Fatal(err)
	}
	if err := registry.MarkAttemptUnknown(ctx, installed.ConfigID, "attempt"); err != nil {
		t.Fatal(err)
	}
	snapshot := registry.Snapshot(installed.ConfigID)
	if snapshot.Plan.Nodes["apply"].Status != model.NodeDraining || snapshot.Attempts[0].Status != model.AttemptUnknown {
		t.Fatalf("timeout was not unknown/draining: %#v", snapshot)
	}
	if _, ready := registry.ReadyOperations(installed.ConfigID); len(ready) != 0 {
		t.Fatalf("unknown timeout did not block: %#v", ready)
	}

	recovered := NewPlanRegistry(store)
	if err := recovered.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ready := recovered.ReadyOperations(installed.ConfigID); len(ready) != 0 {
		t.Fatalf("timeout barrier not durable: %#v", ready)
	}
}
