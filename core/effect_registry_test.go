package core

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/akzj/converge/pkg/model"
)

func TestPlanRegistryPreservesEffectsAcrossRestoreAndTransition(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryExecutionStore()
	id := model.ConfigID{Name: "config"}
	effect := validEffect()
	reference := EffectReference{ID: "ref", EffectID: effect.ID, ConfigID: id, PlanID: "plan", Generation: 1, EffectKey: "download", State: EffectReferenceActive}
	control := EffectControl{ID: "control", ConfigID: id, ProviderType: effect.ProviderType, ProviderDigest: effect.ProviderDigest, Kind: EffectControlObserve, State: EffectControlPending, EffectID: effect.ID, ReferenceID: reference.ID, NextCheckAt: time.Now()}
	plan := &model.Plan{ID: "plan", ConfigID: id, Generation: 1, Desired: model.DesiredState{ConfigID: id}, ProviderType: effect.ProviderType, ProviderDigest: effect.ProviderDigest, Nodes: map[model.OperationKey]*model.Node{}}
	snapshot := ExecutionSnapshot{Revision: 1, Plan: plan, Effects: []ActiveEffect{effect}, EffectReferences: []EffectReference{reference}, EffectControls: []EffectControl{control}}
	if err := store.CommitExecutionCAS(ctx, id, 0, snapshot); err != nil {
		t.Fatal(err)
	}

	registry := NewPlanRegistry(store)
	if err := registry.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	// An unrelated durable outbox transition must preserve effect state.
	if err := registry.EnqueueOutbox(ctx, model.Event{EventID: "event", ConfigID: id.Name}); err != nil {
		t.Fatal(err)
	}

	recovered := NewPlanRegistry(store)
	if err := recovered.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	recovered.mu.RLock()
	state := recovered.configs[id.Name]
	gotEffect, effectOK := state.effects[effect.ID]
	gotReference, referenceOK := state.references[reference.ID]
	gotControl, controlOK := state.controls[control.ID]
	recovered.mu.RUnlock()
	if !effectOK || !referenceOK || !controlOK {
		t.Fatalf("effect state lost: effect=%v ref=%v control=%v", effectOK, referenceOK, controlOK)
	}
	if string(gotEffect.EnsureSpec) != string(effect.EnsureSpec) || gotReference.EffectID != effect.ID || gotControl.ReferenceID != reference.ID {
		t.Fatalf("effect state changed: %#v %#v %#v", gotEffect, gotReference, gotControl)
	}
}

func TestPlanRegistryRestoreRejectsInvalidEffectSnapshot(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryExecutionStore()
	id := model.ConfigID{Name: "invalid"}
	plan := &model.Plan{ID: "plan", ConfigID: id, Generation: 1, Nodes: map[model.OperationKey]*model.Node{}}
	effect := validEffect()
	effect.EnsureSpec = json.RawMessage(`{"x":1}`)
	invalid := ExecutionSnapshot{Revision: 1, Plan: plan, Effects: []ActiveEffect{effect}, EffectReferences: []EffectReference{{ID: "ref", EffectID: "missing", ConfigID: id, PlanID: plan.ID, Generation: 1, EffectKey: "download", State: EffectReferenceActive}}}
	if err := store.CommitExecutionCAS(ctx, id, 0, invalid); err != nil {
		t.Fatal(err)
	}
	if err := NewPlanRegistry(store).Restore(ctx); err == nil {
		t.Fatal("expected invalid snapshot restore error")
	}
}
