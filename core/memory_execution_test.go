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

func TestMemoryExecutionStoreDeepCopiesAcceptedDesired(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryExecutionStore()
	id := model.ConfigID{Name: "config"}
	desired := model.DesiredState{ConfigID: id, ProviderType: "test", Version: 1, Spec: []byte(`{"v":1}`), DependsOn: []string{"base"}}
	desired.Digest = model.DesiredSpecDigest(desired.Spec)
	if err := store.CommitExecutionCAS(ctx, id, 0, ExecutionSnapshot{Revision: 1, AcceptedDesired: &desired}); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadExecution(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	loaded.AcceptedDesired.Spec[0] = '['
	loaded.AcceptedDesired.DependsOn[0] = "changed"
	fresh, err := store.LoadExecution(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if string(fresh.AcceptedDesired.Spec) != `{"v":1}` || fresh.AcceptedDesired.DependsOn[0] != "base" {
		t.Fatalf("accepted desired aliased: %#v", fresh.AcceptedDesired)
	}
}

func TestMemoryJournalAppendIsIdempotentByEventID(t *testing.T) {
	ctx := context.Background()
	journal := NewMemoryJournal()
	event := model.Event{EventID: "event-1", ConfigID: "config", Observed: model.ObservedState{Properties: []byte("original")}}
	if err := journal.Append(ctx, event); err != nil {
		t.Fatal(err)
	}
	if err := journal.Append(ctx, event); err != nil {
		t.Fatal(err)
	}
	event.Observed.Properties[0] = 'X'
	events, err := journal.Events(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || string(events[0].Observed.Properties) != "original" {
		t.Fatalf("events=%#v", events)
	}
}
