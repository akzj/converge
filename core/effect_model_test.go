package core

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/akzj/converge/pkg/model"
)

func TestExecutionSnapshotDeepCopiesEffectState(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryExecutionStore()
	id := model.ConfigID{Name: "config"}
	snapshot := ExecutionSnapshot{
		Revision:         1,
		Plan:             &model.Plan{ConfigID: id, Generation: 1, Nodes: map[model.OperationKey]*model.Node{}},
		Effects:          []ActiveEffect{{ID: "effect", EnsureSpec: json.RawMessage(`{"url":"original"}`)}},
		EffectReferences: []EffectReference{{ID: "reference", EffectID: "effect", ConfigID: id}},
		EffectControls:   []EffectControl{{ID: "control", EffectID: "effect", ReferenceID: "reference", NextCheckAt: time.Now()}},
	}
	if err := store.CommitExecutionCAS(ctx, id, 0, snapshot); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadExecution(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	loaded.Effects[0].EnsureSpec[8] = 'X'
	loaded.EffectReferences[0].ID = "changed"
	loaded.EffectControls[0].ID = "changed"
	fresh, err := store.LoadExecution(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if string(fresh.Effects[0].EnsureSpec) != `{"url":"original"}` {
		t.Fatalf("effect aliased: %s", fresh.Effects[0].EnsureSpec)
	}
	if fresh.EffectReferences[0].ID != "reference" || fresh.EffectControls[0].ID != "control" {
		t.Fatalf("snapshot slices aliased: %#v", fresh)
	}
}
