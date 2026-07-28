package core

import (
	"context"
	"testing"
	"time"

	"github.com/akzj/converge/pkg/model"
)

func TestWaitingTransitionIsDurableAndUsesFreshAttempt(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryExecutionStore()
	registry := NewPlanRegistry(store)
	installed, _, err := registry.Install(context.Background(), 0, testPlan(t, "digest", model.Operation{Key: "wait"}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.StartAttempt(context.Background(), installed.ConfigID, installed.Generation, "wait", "attempt-1"); err != nil {
		t.Fatal(err)
	}

	due := time.Now().Add(time.Minute)
	event := model.Event{ConfigID: "config", NodeKey: "wait", AttemptID: "attempt-1", State: model.StepWaiting, Result: model.StepResult{State: model.StepWaiting, NextCheckAt: due}}
	if err := registry.ApplyWaiting(ctx, event); err != nil {
		t.Fatal(err)
	}
	snapshot := registry.Snapshot(installed.ConfigID)
	if snapshot.Plan.Nodes["wait"].Status != model.NodeWaiting || snapshot.Attempts[0].Status != model.AttemptWaiting {
		t.Fatalf("waiting transition failed: %#v", snapshot)
	}

	if err := registry.WakeDueWaiting(ctx, due.Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if got := registry.Snapshot(installed.ConfigID).Plan.Nodes["wait"].Status; got != model.NodeWaiting {
		t.Fatalf("woke early: %s", got)
	}
	if err := registry.WakeDueWaiting(ctx, due); err != nil {
		t.Fatal(err)
	}
	if got := registry.Snapshot(installed.ConfigID).Plan.Nodes["wait"].Status; got != model.NodePending {
		t.Fatalf("did not wake: %s", got)
	}
	if _, err := registry.StartAttempt(context.Background(), installed.ConfigID, installed.Generation, "wait", "attempt-2"); err != nil {
		t.Fatal(err)
	}

	recovered := NewPlanRegistry(store)
	if err := recovered.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if got := recovered.Snapshot(installed.ConfigID).Plan.Nodes["wait"].AttemptID; got != "attempt-2" {
		t.Fatalf("fresh attempt not durable: %s", got)
	}
}

func TestWaitingRequiresNextCheckTime(t *testing.T) {
	registry := NewPlanRegistry()
	if err := registry.ApplyWaiting(context.Background(), model.Event{AttemptID: "attempt"}); err == nil {
		t.Fatal("expected validation error")
	}
}
