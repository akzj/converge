package core

import (
	"context"
	"testing"

	"github.com/akzj/converge/pkg/model"
)

func TestOutboxIsDurableUntilAcknowledged(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryExecutionStore()
	registry := NewPlanRegistry(store)
	installed, _, err := registry.Install(context.Background(), 0, testPlan(t, "digest", model.Operation{Key: "apply"}))
	if err != nil {
		t.Fatal(err)
	}
	event := model.Event{EventID: "event-1", ConfigID: installed.ConfigID.Name, PlanID: installed.ID, Generation: installed.Generation, NodeKey: "apply", AttemptID: "attempt-1", State: model.StepCompleted}
	if err := registry.EnqueueOutbox(ctx, event); err != nil {
		t.Fatal(err)
	}

	recovered := NewPlanRegistry(store)
	if err := recovered.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	pending := recovered.PendingOutbox()
	if len(pending) != 1 || pending[0].EventID != "event-1" {
		t.Fatalf("outbox not recovered: %#v", pending)
	}
	if err := recovered.AckOutbox(ctx, installed.ConfigID, "event-1"); err != nil {
		t.Fatal(err)
	}
	if pending := recovered.PendingOutbox(); len(pending) != 0 {
		t.Fatalf("outbox not acknowledged: %#v", pending)
	}

	recoveredAgain := NewPlanRegistry(store)
	if err := recoveredAgain.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if pending := recoveredAgain.PendingOutbox(); len(pending) != 0 {
		t.Fatalf("ack not durable: %#v", pending)
	}
}

func TestOutboxEnqueueIsIdempotentByEventID(t *testing.T) {
	ctx := context.Background()
	registry := NewPlanRegistry(NewMemoryExecutionStore())
	installed, _, err := registry.Install(context.Background(), 0, testPlan(t, "digest", model.Operation{Key: "apply"}))
	if err != nil {
		t.Fatal(err)
	}
	event := model.Event{EventID: "same", ConfigID: installed.ConfigID.Name}
	if err := registry.EnqueueOutbox(ctx, event); err != nil {
		t.Fatal(err)
	}
	if err := registry.EnqueueOutbox(ctx, event); err != nil {
		t.Fatal(err)
	}
	if got := len(registry.PendingOutbox()); got != 1 {
		t.Fatalf("pending=%d, want 1", got)
	}
}
