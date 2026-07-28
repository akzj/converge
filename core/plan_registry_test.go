package core

import (
	"errors"
	"testing"

	"github.com/akzj/converge/pkg/model"
)

func TestPlanRegistryGenerationCASPreservesActivePlan(t *testing.T) {
	r := NewPlanRegistry()
	first := testPlan(t, "digest", model.Operation{Key: "apply", Action: "one"})
	installed, _, err := r.Install(0, first)
	if err != nil {
		t.Fatal(err)
	}
	if installed.Generation != 1 || installed.ID == "" {
		t.Fatalf("invalid installed identity: %#v", installed)
	}

	stale := testPlan(t, "digest", model.Operation{Key: "apply", Action: "two"})
	if _, _, err := r.Install(0, stale); !errors.Is(err, ErrGenerationChanged) {
		t.Fatalf("got %v, want generation error", err)
	}
	got := r.Snapshot(model.ConfigID{Name: "config"}).Plan
	if got.Generation != 1 || got.Nodes["apply"].Operation.Action != "one" {
		t.Fatalf("stale install damaged active plan: %#v", got)
	}
}

func TestPlanRegistryCarriesMatchingRunningAttempt(t *testing.T) {
	r := NewPlanRegistry()
	first, _, err := r.Install(0, testPlan(t, "digest", model.Operation{Key: "apply", Action: "same"}))
	if err != nil {
		t.Fatal(err)
	}

	r.mu.Lock()
	state, node := r.configs["config"], r.configs["config"].active.Nodes["apply"]
	node.Status, node.AttemptID = model.NodeRunning, "attempt-1"
	state.attempts["attempt-1"] = &model.Attempt{ID: "attempt-1", PlanID: first.ID, Generation: 1, ConfigID: first.ConfigID, NodeKey: "apply", Fingerprint: node.Operation.Fingerprint, Status: model.AttemptRunning}
	r.mu.Unlock()

	installed, change, err := r.Install(1, testPlan(t, "digest", model.Operation{Key: "apply", Action: "same"}, model.Operation{Key: "new", Action: "add"}))
	if err != nil {
		t.Fatal(err)
	}
	if installed.Nodes["apply"].AttemptID != "attempt-1" || installed.Nodes["apply"].Status != model.NodeRunning {
		t.Fatalf("running node not carried: %#v", installed.Nodes["apply"])
	}
	if len(change.Carry) != 1 || change.Carry[0] != "apply" {
		t.Fatalf("unexpected change: %#v", change)
	}
	snapshot := r.Snapshot(first.ConfigID)
	for _, attempt := range snapshot.Attempts {
		if attempt.ID == "attempt-1" && (attempt.Generation != 2 || attempt.CarriedTo != 2) {
			t.Fatalf("attempt not migrated: %#v", attempt)
		}
	}
}

func TestPlanRegistryRejectsRunningCarryWithoutTrackedAttempt(t *testing.T) {
	r := NewPlanRegistry()
	if _, _, err := r.Install(0, testPlan(t, "digest", model.Operation{Key: "apply", Action: "same"})); err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	r.configs["config"].active.Nodes["apply"].Status = model.NodeRunning
	r.configs["config"].active.Nodes["apply"].AttemptID = "missing"
	r.mu.Unlock()
	if _, _, err := r.Install(1, testPlan(t, "digest", model.Operation{Key: "apply", Action: "same"})); err == nil {
		t.Fatal("expected missing attempt error")
	}
	if got := r.Snapshot(model.ConfigID{Name: "config"}).Plan.Generation; got != 1 {
		t.Fatalf("failed install changed generation to %d", got)
	}
}

func TestPlanRegistrySnapshotIsDeepCopy(t *testing.T) {
	r := NewPlanRegistry()
	if _, _, err := r.Install(0, testPlan(t, "digest", model.Operation{Key: "apply", Action: "same", Input: []byte(`{"a":1}`)})); err != nil {
		t.Fatal(err)
	}
	snapshot := r.Snapshot(model.ConfigID{Name: "config"})
	snapshot.Plan.Nodes["apply"].Operation.Input[0] = '['
	if got := string(r.Snapshot(model.ConfigID{Name: "config"}).Plan.Nodes["apply"].Operation.Input); got != `{"a":1}` {
		t.Fatalf("snapshot mutated registry: %s", got)
	}
}

func TestPlanRegistryAttemptStartsAtMostOnce(t *testing.T) {
	r := NewPlanRegistry()
	installed, _, err := r.Install(0, testPlan(t, "digest", model.Operation{Key: "apply"}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.StartAttempt(installed.ConfigID, installed.Generation, "apply", "attempt-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.StartAttempt(installed.ConfigID, installed.Generation, "apply", "attempt-2"); err == nil {
		t.Fatal("duplicate start succeeded")
	}
}

func TestPlanRegistryEventIdentityAndIdempotency(t *testing.T) {
	r := NewPlanRegistry()
	installed, _, err := r.Install(0, testPlan(t, "digest", model.Operation{Key: "apply"}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.StartAttempt(installed.ConfigID, installed.Generation, "apply", "attempt-1"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.ApplyEvent(model.Event{ConfigID: "config", NodeKey: "other", AttemptID: "attempt-1", State: model.StepCompleted}); err == nil {
		t.Fatal("mismatched event succeeded")
	}
	event := model.Event{ConfigID: "config", PlanID: installed.ID, Generation: installed.Generation, NodeKey: "apply", AttemptID: "attempt-1", State: model.StepCompleted}
	changed, _, err := r.ApplyEvent(event)
	if err != nil || !changed {
		t.Fatalf("completion failed: changed=%v err=%v", changed, err)
	}
	changed, _, err = r.ApplyEvent(event)
	if err != nil || changed {
		t.Fatalf("duplicate event was not idempotent: changed=%v err=%v", changed, err)
	}
	if got := r.Snapshot(installed.ConfigID).Plan.Nodes["apply"].Status; got != model.NodeCompleted {
		t.Fatalf("status=%s, want completed", got)
	}
}

func TestPlanRegistryRejectsNonMonotonicDesiredRevision(t *testing.T) {
	r := NewPlanRegistry()
	first, err := BuildCandidate(model.ConfigID{Name: "config"}, model.DesiredState{Version: 2, Digest: "v2"}, "test", "digest", []model.Operation{{Key: "apply"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.Install(0, first); err != nil {
		t.Fatal(err)
	}
	for _, desired := range []model.DesiredState{{Version: 1, Digest: "v1"}, {Version: 2, Digest: "different"}} {
		candidate, err := BuildCandidate(model.ConfigID{Name: "config"}, desired, "test", "digest", []model.Operation{{Key: "apply"}})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := r.Install(1, candidate); !errors.Is(err, ErrDesiredConflict) {
			t.Fatalf("desired %#v error=%v, want conflict", desired, err)
		}
	}
}

func TestPlanRegistryUnknownOldEventCannotMutateActive(t *testing.T) {
	r := NewPlanRegistry()
	installed, _, err := r.Install(0, testPlan(t, "digest", model.Operation{Key: "apply"}))
	if err != nil {
		t.Fatal(err)
	}
	changed, retired, err := r.ApplyEvent(model.Event{ConfigID: "config", NodeKey: "apply", AttemptID: "old-attempt", State: model.StepCompleted})
	if err != nil || changed || retired {
		t.Fatalf("unknown event changed state: %v %v %v", changed, retired, err)
	}
	if got := r.Snapshot(installed.ConfigID).Plan.Nodes["apply"].Status; got != model.NodePending {
		t.Fatalf("unknown event mutated node: %s", got)
	}
}
