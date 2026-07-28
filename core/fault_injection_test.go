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
func (s *failingExecutionStore) CommitExecutionCAS(ctx context.Context, id model.ConfigID, expected model.Generation, snapshot ExecutionSnapshot) error {
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
	snapshot := registry.Snapshot(installed.ConfigID)
	if node := snapshot.Plan.Nodes["apply"]; node.Status != model.NodePending || node.AttemptID != "" {
		t.Fatalf("failed start leaked into memory: %#v", node)
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
	event := model.Event{EventID: "event", ConfigID: installed.ConfigID.Name}
	if err := registry.EnqueueOutbox(context.Background(), event); err == nil {
		t.Fatal("expected injected persistence failure")
	}
	if events := registry.PendingOutbox(); len(events) != 0 {
		t.Fatalf("failed enqueue leaked into memory: %#v", events)
	}
}
