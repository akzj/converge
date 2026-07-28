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

func TestOutboxSurvivesUnrelatedRegistryTransitions(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryExecutionStore()
	registry := NewPlanRegistry(store)
	installed, _, err := registry.Install(ctx, 0, testPlan(t, "digest", model.Operation{Key: "first"}, model.Operation{Key: "second"}))
	if err != nil {
		t.Fatal(err)
	}
	first := model.Event{EventID: "first-event", ConfigID: installed.ConfigID.Name, Observed: model.ObservedState{Properties: []byte("original")}}
	second := model.Event{EventID: "second-event", ConfigID: installed.ConfigID.Name}
	if err := registry.EnqueueOutbox(ctx, first); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.StartAttempt(ctx, installed.ConfigID, installed.Generation, "second", "attempt-2"); err != nil {
		t.Fatal(err)
	}
	if err := registry.EnqueueOutbox(ctx, second); err != nil {
		t.Fatal(err)
	}
	first.Observed.Properties[0] = 'X'

	pending := registry.PendingOutbox()
	if len(pending) != 2 {
		t.Fatalf("pending=%d, want 2: %#v", len(pending), pending)
	}
	if err := registry.AckOutbox(ctx, installed.ConfigID, "second-event"); err != nil {
		t.Fatal(err)
	}
	pending = registry.PendingOutbox()
	if len(pending) != 1 || pending[0].EventID != "first-event" || string(pending[0].Observed.Properties) != "original" {
		t.Fatalf("unrelated transition lost or aliased event: %#v", pending)
	}

	recovered := NewPlanRegistry(store)
	if err := recovered.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if pending := recovered.PendingOutbox(); len(pending) != 1 || pending[0].EventID != "first-event" {
		t.Fatalf("event not durable: %#v", pending)
	}
}
