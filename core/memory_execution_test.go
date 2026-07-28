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
	snapshot := ExecutionSnapshot{Plan: &model.Plan{ConfigID: id, Generation: 1, Nodes: map[model.OperationKey]*model.Node{"apply": {Operation: model.Operation{Key: "apply", Input: []byte(`{"a":1}`)}}}}, Attempts: []model.Attempt{{ID: "attempt"}}}
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
