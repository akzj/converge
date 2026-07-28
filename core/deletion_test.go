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
