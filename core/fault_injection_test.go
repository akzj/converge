package core

import (
	"context"
	"errors"
	"testing"

	"github.com/akzj/converge/pkg/model"
)

type failingExecutionStore struct {
	inner *MemoryExecutionStore
	fail  bool
}

func (s *failingExecutionStore) LoadExecution(ctx context.Context, id model.ConfigID) (*ExecutionSnapshot, error) {
	return s.inner.LoadExecution(ctx, id)
}
func (s *failingExecutionStore) ListExecutions(ctx context.Context) ([]model.ConfigID, error) {
	return s.inner.ListExecutions(ctx)
}
func (s *failingExecutionStore) CommitExecutionCAS(ctx context.Context, id model.ConfigID, expected uint64, snapshot ExecutionSnapshot) error {
	if s.fail {
		return errors.New("injected commit failure")
	}
	return s.inner.CommitExecutionCAS(ctx, id, expected, snapshot)
}
func (s *failingExecutionStore) DeleteExecution(ctx context.Context, id model.ConfigID) error {
	return s.inner.DeleteExecution(ctx, id)
}

func TestPersistenceFailureDoesNotPublishPlanInMemory(t *testing.T) {
	store := &failingExecutionStore{inner: NewMemoryExecutionStore(), fail: true}
	registry := NewPlanRegistry(store)
	candidate := testPlan(t, "digest", model.Operation{Key: "apply"})
	if _, _, err := registry.Install(context.Background(), 0, candidate); err == nil {
		t.Fatal("expected injected persistence failure")
	}
	if snapshot := registry.Snapshot(candidate.ConfigID); snapshot.Plan != nil {
		t.Fatalf("failed install leaked into memory: %#v", snapshot)
	}
}

func TestSubmitDesiredDoesNotAckPersistenceFailure(t *testing.T) {
	store := &failingExecutionStore{inner: NewMemoryExecutionStore(), fail: true}
	r := NewReconciler(NewMemoryStateStore(), store, NewMemoryEventBus(), NewMemoryArbiter(), NewMemoryJournal())
	desired := model.DesiredState{
		ConfigID: model.ConfigID{Name: "config"}, ProviderType: "missing", Version: 1,
		Spec: []byte(`{"v":1}`),
	}
	desired.Digest = model.DesiredSpecDigest(desired.Spec)
	if err := r.SubmitDesired(context.Background(), desired); err == nil {
		t.Fatal("expected durable acceptance failure")
	}
	if _, ok := r.Config(desired.ConfigID.Name); ok {
		t.Fatal("failed durable acceptance leaked into reconciler state")
	}
}

func TestPersistenceFailureDoesNotStartAttemptInMemory(t *testing.T) {
	store := &failingExecutionStore{inner: NewMemoryExecutionStore()}
	registry := NewPlanRegistry(store)
	installed, _, err := registry.Install(context.Background(), 0, testPlan(t, "digest", model.Operation{Key: "apply"}))
	if err != nil {
		t.Fatal(err)
	}
	store.fail = true
	if _, err := registry.StartAttempt(context.Background(), installed.ConfigID, installed.Generation, "apply", "attempt-1"); err == nil {
		t.Fatal("expected injected persistence failure")
	}
	if node := registry.Snapshot(installed.ConfigID).Plan.Nodes["apply"]; node.Status != model.NodePending || node.AttemptID != "" {
		t.Fatalf("failed start leaked: %#v", node)
	}
}

func TestPersistenceFailureDoesNotEnqueueOutboxInMemory(t *testing.T) {
	store := &failingExecutionStore{inner: NewMemoryExecutionStore()}
	registry := NewPlanRegistry(store)
	installed, _, err := registry.Install(context.Background(), 0, testPlan(t, "digest", model.Operation{Key: "apply"}))
	if err != nil {
		t.Fatal(err)
	}
	store.fail = true
	if err := registry.EnqueueOutbox(context.Background(), model.Event{EventID: "event", ConfigID: installed.ConfigID.Name}); err == nil {
		t.Fatal("expected injected persistence failure")
	}
	if events := registry.PendingOutbox(); len(events) != 0 {
		t.Fatalf("failed enqueue leaked: %#v", events)
	}
}

func TestEventTransitionFailureRetainsOutboxForRetry(t *testing.T) {
	ctx := context.Background()
	store := &failingExecutionStore{inner: NewMemoryExecutionStore()}
	registry := NewPlanRegistry(store)
	installed, _, err := registry.Install(ctx, 0, testPlan(t, "digest", model.Operation{Key: "apply"}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.StartAttempt(ctx, installed.ConfigID, installed.Generation, "apply", "attempt-1"); err != nil {
		t.Fatal(err)
	}
	event := model.Event{EventID: "event-1", ConfigID: installed.ConfigID.Name, PlanID: installed.ID, Generation: installed.Generation, NodeKey: "apply", AttemptID: "attempt-1", State: model.StepCompleted, Result: model.StepResult{State: model.StepCompleted}}
	if err := registry.EnqueueOutbox(ctx, event); err != nil {
		t.Fatal(err)
	}
	r := NewReconciler(NewMemoryStateStore(), store, NewMemoryEventBus(), NewMemoryArbiter(), NewMemoryJournal())
	r.registry = registry
	store.fail = true
	r.handleEvent(ctx, event)
	if pending := registry.PendingOutbox(); len(pending) != 1 {
		t.Fatalf("failed transition acknowledged: %#v", pending)
	}
	if status := registry.Snapshot(installed.ConfigID).Plan.Nodes["apply"].Status; status != model.NodeRunning {
		t.Fatalf("failed transition status: %s", status)
	}
	store.fail = false
	r.handleEvent(ctx, event)
	if pending := registry.PendingOutbox(); len(pending) != 0 {
		t.Fatalf("successful retry did not acknowledge: %#v", pending)
	}
	if status := registry.Snapshot(installed.ConfigID).Plan.Nodes["apply"].Status; status != model.NodeCompleted {
		t.Fatalf("retry status=%s", status)
	}
}

func TestInvalidEffectStateCannotBePersistedByUnrelatedTransition(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryExecutionStore()
	registry := NewPlanRegistry(store)
	installed, _, err := registry.Install(ctx, 0, testPlan(t, "digest", model.Operation{Key: "apply"}))
	if err != nil {
		t.Fatal(err)
	}
	registry.mu.Lock()
	state := registry.configs[installed.ConfigID.Name]
	state.effects["bad"] = ActiveEffect{ID: "bad", Binding: EffectBindingBound, State: ExternalEffectActive}
	beforeRevision := state.revision
	registry.mu.Unlock()
	if err := registry.EnqueueOutbox(ctx, model.Event{EventID: "event", ConfigID: installed.ConfigID.Name}); err == nil {
		t.Fatal("expected validation failure")
	}
	registry.mu.RLock()
	afterRevision := registry.configs[installed.ConfigID.Name].revision
	registry.mu.RUnlock()
	if afterRevision != beforeRevision {
		t.Fatalf("revision changed: %d -> %d", beforeRevision, afterRevision)
	}
	stored, err := store.LoadExecution(ctx, installed.ConfigID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Revision != beforeRevision || len(stored.Effects) != 0 || len(stored.Outbox) != 0 {
		t.Fatalf("invalid state reached store: %#v", stored)
	}
}
