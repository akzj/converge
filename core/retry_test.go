package core

import (
	"context"
	"fmt"
	"testing"

	"github.com/akzj/converge/pkg/model"
)

func TestRetryableFailureUsesFreshAttemptAndStopsAtLimit(t *testing.T) {
	ctx := context.Background()
	registry := NewPlanRegistry(NewMemoryExecutionStore())
	installed, _, err := registry.Install(0, testPlan(t, "digest", model.Operation{Key: "apply"}))
	if err != nil {
		t.Fatal(err)
	}

	for number := 1; number <= maxAttemptsPerNode; number++ {
		attemptID := model.AttemptID(fmt.Sprintf("attempt-%d", number))
		if _, err := registry.StartAttempt(installed.ConfigID, installed.Generation, "apply", attemptID); err != nil {
			t.Fatal(err)
		}
		event := model.Event{ConfigID: "config", NodeKey: "apply", AttemptID: attemptID, State: model.StepFailed, Result: model.StepResult{State: model.StepFailed, Retryable: true}}
		retried, exhausted, err := registry.ApplyRetryableFailure(ctx, event)
		if err != nil {
			t.Fatal(err)
		}
		if number < maxAttemptsPerNode {
			if !retried || exhausted {
				t.Fatalf("attempt %d: retried=%v exhausted=%v", number, retried, exhausted)
			}
			if got := registry.Snapshot(installed.ConfigID).Plan.Nodes["apply"].AttemptID; got != "" {
				t.Fatalf("old attempt remained active: %s", got)
			}
		} else if retried || !exhausted {
			t.Fatalf("limit: retried=%v exhausted=%v", retried, exhausted)
		}
	}
}

func TestRetryableDuplicateFailureIsIdempotent(t *testing.T) {
	ctx := context.Background()
	registry := NewPlanRegistry()
	installed, _, err := registry.Install(0, testPlan(t, "digest", model.Operation{Key: "apply"}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.StartAttempt(installed.ConfigID, installed.Generation, "apply", "attempt-1"); err != nil {
		t.Fatal(err)
	}
	event := model.Event{ConfigID: "config", NodeKey: "apply", AttemptID: "attempt-1", State: model.StepFailed, Result: model.StepResult{State: model.StepFailed, Retryable: true}}
	if _, _, err := registry.ApplyRetryableFailure(ctx, event); err != nil {
		t.Fatal(err)
	}
	retried, exhausted, err := registry.ApplyRetryableFailure(ctx, event)
	if err != nil || retried || exhausted {
		t.Fatalf("duplicate changed state: %v %v %v", retried, exhausted, err)
	}
}
