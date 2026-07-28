package core

import (
	"context"
	"errors"
	"testing"

	"github.com/akzj/converge/pkg/model"
)

func TestMemoryExecutionStoreCASAndDeepCopy(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryExecutionStore()
	id := model.ConfigID{Name: "config"}
	snapshot := ExecutionSnapshot{Revision: 1, Plan: &model.Plan{ConfigID: id, Generation: 1, Nodes: map[model.OperationKey]*model.Node{"apply": {Operation: model.Operation{Key: "apply", Input: []byte(`{"a":1}`)}}}}, Attempts: []model.Attempt{{ID: "attempt"}}}
	if err := store.CommitExecutionCAS(ctx, id, 0, snapshot); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitExecutionCAS(ctx, id, 0, snapshot); !errors.Is(err, ErrGenerationChanged) {
		t.Fatalf("stale CAS error=%v", err)
	}

	loaded, err := store.LoadExecution(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	loaded.Plan.Nodes["apply"].Operation.Input[0] = '['
	loaded.Attempts[0].ID = "changed"
	fresh, err := store.LoadExecution(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if string(fresh.Plan.Nodes["apply"].Operation.Input) != `{"a":1}` || fresh.Attempts[0].ID != "attempt" {
		t.Fatalf("load was not a deep copy: %#v", fresh)
	}
}

func TestMemoryExecutionStoreRejectsStaleSameGenerationTransition(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryExecutionStore()
	id := model.ConfigID{Name: "config"}
	first := ExecutionSnapshot{Revision: 1, Plan: &model.Plan{ConfigID: id, Generation: 7, Nodes: map[model.OperationKey]*model.Node{}}}
	if err := store.CommitExecutionCAS(ctx, id, 0, first); err != nil {
		t.Fatal(err)
	}
	second := ExecutionSnapshot{Revision: 2, Plan: first.Plan.Clone()}
	if err := store.CommitExecutionCAS(ctx, id, 1, second); err != nil {
		t.Fatal(err)
	}
	stale := ExecutionSnapshot{Revision: 2, Plan: first.Plan.Clone()}
	if err := store.CommitExecutionCAS(ctx, id, 1, stale); !errors.Is(err, ErrGenerationChanged) {
		t.Fatalf("same-generation stale update error=%v", err)
	}
	loaded, err := store.LoadExecution(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Revision != 2 || loaded.Plan.Generation != 7 {
		t.Fatalf("snapshot=%#v", loaded)
	}
}
