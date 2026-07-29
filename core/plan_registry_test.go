package core

import (
	"context"
	"errors"
	"testing"

	"github.com/akzj/converge/pkg/model"
)

func TestPlanRegistryGenerationCASPreservesActivePlan(t *testing.T) {
	r := NewPlanRegistry()
	first := testPlan(t, "digest", model.Operation{Key: "apply", ExecutionKind: model.ExecutionDirect, Action: "one"})
	installed, _, err := r.Install(context.Background(), 0, first)
	if err != nil {
		t.Fatal(err)
	}
	if installed.Generation != 1 || installed.ID == "" {
		t.Fatalf("invalid installed identity: %#v", installed)
	}

	stale := testPlan(t, "digest", model.Operation{Key: "apply", ExecutionKind: model.ExecutionDirect, Action: "two"})
	if _, _, err := r.Install(context.Background(), 0, stale); !errors.Is(err, ErrGenerationChanged) {
		t.Fatalf("got %v, want generation error", err)
	}
	got := r.Snapshot(model.ConfigID{Name: "config"}).Plan
	if got.Generation != 1 || got.Nodes["apply"].Operation.Action != "one" {
		t.Fatalf("stale install damaged active plan: %#v", got)
	}
}

func TestPlanRegistryCarriesMatchingRunningAttempt(t *testing.T) {
	r := NewPlanRegistry()
	first, _, err := r.Install(context.Background(), 0, testPlan(t, "digest", model.Operation{Key: "apply", ExecutionKind: model.ExecutionDirect, Action: "same"}))
	if err != nil {
		t.Fatal(err)
	}

	r.mu.Lock()
	state, node := r.configs["config"], r.configs["config"].active.Nodes["apply"]
	node.Status, node.AttemptID = model.NodeRunning, "attempt-1"
	state.attempts["attempt-1"] = &model.Attempt{ID: "attempt-1", PlanID: first.ID, Generation: 1, ConfigID: first.ConfigID, NodeKey: "apply", Fingerprint: node.Operation.Fingerprint, Status: model.AttemptRunning}
	r.mu.Unlock()

	installed, change, err := r.Install(context.Background(), 1, testPlan(t, "digest", model.Operation{Key: "apply", ExecutionKind: model.ExecutionDirect, Action: "same"}, model.Operation{Key: "new", Action: "add"}))
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
		if attempt.ID == "attempt-1" && (attempt.Generation != 1 || attempt.CarriedTo != 2) {
			t.Fatalf("attempt not migrated: %#v", attempt)
		}
	}
}

func TestPlanRegistryRejectsRunningCarryWithoutTrackedAttempt(t *testing.T) {
	r := NewPlanRegistry()
	if _, _, err := r.Install(context.Background(), 0, testPlan(t, "digest", model.Operation{Key: "apply", ExecutionKind: model.ExecutionDirect, Action: "same"})); err != nil {
		t.Fatal(err)
	}
	r.mu.Lock()
	r.configs["config"].active.Nodes["apply"].Status = model.NodeRunning
	r.configs["config"].active.Nodes["apply"].AttemptID = "missing"
	r.mu.Unlock()
	if _, _, err := r.Install(context.Background(), 1, testPlan(t, "digest", model.Operation{Key: "apply", ExecutionKind: model.ExecutionDirect, Action: "same"})); err == nil {
		t.Fatal("expected missing attempt error")
	}
	if got := r.Snapshot(model.ConfigID{Name: "config"}).Plan.Generation; got != 1 {
		t.Fatalf("failed install changed generation to %d", got)
	}
}

func TestPlanRegistrySnapshotIsDeepCopy(t *testing.T) {
	r := NewPlanRegistry()
	if _, _, err := r.Install(context.Background(), 0, testPlan(t, "digest", model.Operation{Key: "apply", ExecutionKind: model.ExecutionDirect, Action: "same", Input: []byte(`{"a":1}`)})); err != nil {
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
	installed, _, err := r.Install(context.Background(), 0, testPlan(t, "digest", model.Operation{Key: "apply", ExecutionKind: model.ExecutionDirect}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.StartAttempt(context.Background(), installed.ConfigID, installed.Generation, "apply", "attempt-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.StartAttempt(context.Background(), installed.ConfigID, installed.Generation, "apply", "attempt-2"); err == nil {
		t.Fatal("duplicate start succeeded")
	}
}

func TestPlanRegistryEventIdentityAndIdempotency(t *testing.T) {
	r := NewPlanRegistry()
	installed, _, err := r.Install(context.Background(), 0, testPlan(t, "digest", model.Operation{Key: "apply", ExecutionKind: model.ExecutionDirect}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.StartAttempt(context.Background(), installed.ConfigID, installed.Generation, "apply", "attempt-1"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.ApplyEvent(context.Background(), model.Event{ConfigID: "config", NodeKey: "other", AttemptID: "attempt-1", State: model.StepCompleted}); err == nil {
		t.Fatal("mismatched event succeeded")
	}
	event := model.Event{ConfigID: "config", PlanID: installed.ID, Generation: installed.Generation, NodeKey: "apply", AttemptID: "attempt-1", State: model.StepCompleted}
	changed, _, err := r.ApplyEvent(context.Background(), event)
	if err != nil || !changed {
		t.Fatalf("completion failed: changed=%v err=%v", changed, err)
	}
	changed, _, err = r.ApplyEvent(context.Background(), event)
	if err != nil || changed {
		t.Fatalf("duplicate event was not idempotent: changed=%v err=%v", changed, err)
	}
	if got := r.Snapshot(installed.ConfigID).Plan.Nodes["apply"].Status; got != model.NodeCompleted {
		t.Fatalf("status=%s, want completed", got)
	}
}

func TestPlanRegistryRejectsNonMonotonicDesiredRevision(t *testing.T) {
	r := NewPlanRegistry()
	first, err := BuildCandidate(model.ConfigID{Name: "config"}, model.DesiredState{Version: 2, Digest: "v2"}, "test", "digest", []model.Operation{{Key: "apply", ExecutionKind: model.ExecutionDirect}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.Install(context.Background(), 0, first); err != nil {
		t.Fatal(err)
	}
	for _, desired := range []model.DesiredState{{Version: 1, Digest: "v1"}, {Version: 2, Digest: "different"}} {
		candidate, err := BuildCandidate(model.ConfigID{Name: "config"}, desired, "test", "digest", []model.Operation{{Key: "apply", ExecutionKind: model.ExecutionDirect}})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := r.Install(context.Background(), 1, candidate); !errors.Is(err, ErrDesiredConflict) {
			t.Fatalf("desired %#v error=%v, want conflict", desired, err)
		}
	}
}

func TestPlanRegistryUnknownOldEventCannotMutateActive(t *testing.T) {
	r := NewPlanRegistry()
	installed, _, err := r.Install(context.Background(), 0, testPlan(t, "digest", model.Operation{Key: "apply", ExecutionKind: model.ExecutionDirect}))
	if err != nil {
		t.Fatal(err)
	}
	changed, retired, err := r.ApplyEvent(context.Background(), model.Event{ConfigID: "config", NodeKey: "apply", AttemptID: "old-attempt", State: model.StepCompleted})
	if err != nil || changed || retired {
		t.Fatalf("unknown event changed state: %v %v %v", changed, retired, err)
	}
	if got := r.Snapshot(installed.ConfigID).Plan.Nodes["apply"].Status; got != model.NodePending {
		t.Fatalf("unknown event mutated node: %s", got)
	}
}

func TestPlanRegistryRetiredConflictBarrier(t *testing.T) {
	r := NewPlanRegistry()
	old := testPlan(t, "digest", model.Operation{Key: "old", Action: "old", CancelMode: model.CancelModeNone, ConflictKey: "resource/x"})
	installed, _, err := r.Install(context.Background(), 0, old)
	if err != nil {
		t.Fatal(err)
	}
	oldPlanID, oldGeneration := installed.ID, installed.Generation
	if _, err := r.StartAttempt(context.Background(), installed.ConfigID, installed.Generation, "old", "attempt-old"); err != nil {
		t.Fatal(err)
	}
	candidate := testPlan(t, "digest", model.Operation{Key: "blocked", Action: "new", ConflictKey: "resource/x"}, model.Operation{Key: "free", Action: "new", ConflictKey: "resource/y"})
	installed, change, err := r.Install(context.Background(), 1, candidate)
	if err != nil {
		t.Fatal(err)
	}
	if len(change.Drain) != 1 {
		t.Fatalf("expected drain: %#v", change)
	}
	_, ready := r.ReadyOperations(installed.ConfigID)
	if len(ready) != 1 || ready[0].Key != "free" {
		t.Fatalf("barrier ready set=%#v, want only free", ready)
	}
	_, retiredFinished, err := r.ApplyEvent(context.Background(), model.Event{ConfigID: "config", PlanID: oldPlanID, Generation: oldGeneration, NodeKey: "old", AttemptID: "attempt-old", State: model.StepCompleted})
	if err != nil || !retiredFinished {
		t.Fatalf("retired completion failed: %v %v", retiredFinished, err)
	}
	_, ready = r.ReadyOperations(installed.ConfigID)
	if len(ready) != 2 {
		t.Fatalf("barrier not released: %#v", ready)
	}
}

func TestPlanRegistryPersistsAndRestoresRunningAsUnknown(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryExecutionStore()
	first := NewPlanRegistry(store)
	installed, _, err := first.Install(context.Background(), 0, testPlan(t, "digest", model.Operation{Key: "apply", ExecutionKind: model.ExecutionDirect, ConflictKey: "resource/x"}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.StartAttempt(context.Background(), installed.ConfigID, installed.Generation, "apply", "attempt-1"); err != nil {
		t.Fatal(err)
	}

	recovered := NewPlanRegistry(store)
	if err := recovered.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot := recovered.Snapshot(installed.ConfigID)
	if snapshot.Plan == nil || snapshot.Plan.Nodes["apply"].Status != model.NodeDraining {
		t.Fatalf("running node not recovered conservatively: %#v", snapshot.Plan)
	}
	if len(snapshot.Attempts) != 1 || snapshot.Attempts[0].Status != model.AttemptUnknown {
		t.Fatalf("attempt not recovered as unknown: %#v", snapshot.Attempts)
	}
	_, ready := recovered.ReadyOperations(installed.ConfigID)
	if len(ready) != 0 {
		t.Fatalf("unknown effect did not block conflicting work: %#v", ready)
	}
}

func TestPlanRegistryDeleteRemovesMemoryAndDurableState(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryExecutionStore()
	registry := NewPlanRegistry(store)
	installed, _, err := registry.Install(context.Background(), 0, testPlan(t, "digest", model.Operation{Key: "apply", ExecutionKind: model.ExecutionDirect}))
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Delete(ctx, installed.ConfigID); err != nil {
		t.Fatal(err)
	}
	if snapshot := registry.Snapshot(installed.ConfigID); snapshot.Plan != nil {
		t.Fatalf("registry state remains: %#v", snapshot)
	}
	if stored, err := store.LoadExecution(ctx, installed.ConfigID); err != nil || stored != nil {
		t.Fatalf("durable state remains: %#v err=%v", stored, err)
	}
	recovered := NewPlanRegistry(store)
	if err := recovered.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if snapshot := recovered.Snapshot(installed.ConfigID); snapshot.Plan != nil {
		t.Fatalf("deleted state reappeared: %#v", snapshot)
	}
}

func TestAttemptIdentityRejectsReuseAndWrongGeneration(t *testing.T) {
	ctx := context.Background()
	registry := NewPlanRegistry()
	installed, _, err := registry.Install(ctx, 0, testPlan(t, "digest", model.Operation{Key: "apply", ExecutionKind: model.ExecutionDirect}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.StartAttempt(ctx, installed.ConfigID, installed.Generation, "apply", "unique"); err != nil {
		t.Fatal(err)
	}
	wrong := model.Event{ConfigID: "config", PlanID: installed.ID, Generation: installed.Generation + 1, NodeKey: "apply", AttemptID: "unique", State: model.StepCompleted}
	if _, _, err := registry.ApplyEvent(ctx, wrong); err == nil {
		t.Fatal("wrong generation event was accepted")
	}
	correct := wrong
	correct.Generation = installed.Generation
	if _, _, err := registry.ApplyEvent(ctx, correct); err != nil {
		t.Fatal(err)
	}

	// A completed attempt remains reserved and cannot be reused after a waiting/retry-style reset.
	registry.mu.Lock()
	state := registry.configs["config"]
	attempt := state.attempts["unique"]
	state.retired["unique"] = attempt
	delete(state.attempts, "unique")
	state.active.Nodes["apply"].Status, state.active.Nodes["apply"].AttemptID = model.NodePending, ""
	registry.mu.Unlock()
	if _, err := registry.StartAttempt(ctx, installed.ConfigID, installed.Generation, "apply", "unique"); err == nil {
		t.Fatal("retired attempt ID was reused")
	}
}

func TestNewAttemptIDIsRandomAndUnique(t *testing.T) {
	seen := make(map[model.AttemptID]bool)
	for i := 0; i < 1000; i++ {
		id, err := newAttemptID()
		if err != nil {
			t.Fatal(err)
		}
		if len(id) != 32 || seen[id] {
			t.Fatalf("invalid or duplicate ID %q", id)
		}
		seen[id] = true
	}
}
