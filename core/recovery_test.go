package core

import (
	"context"
	"testing"

	"github.com/akzj/converge/pkg/model"
)

func TestRecoveryRebuildsInProgressConfigFromExecutionDesired(t *testing.T) {
	ctx := context.Background()
	executionStore := NewMemoryExecutionStore()
	registry := NewPlanRegistry(executionStore)
	desired := model.DesiredState{ConfigID: model.ConfigID{Name: "first-convergence"}, ProviderType: "recovery", Version: 2, Digest: "v2", Spec: []byte(`{"critical":true}`), DependsOn: []string{"upstream"}}
	candidate, err := BuildCandidate(desired.ConfigID, desired, "recovery", "digest-recovery", []model.Operation{{Key: "apply"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := registry.Install(ctx, 0, candidate); err != nil {
		t.Fatal(err)
	}

	r := NewReconciler(NewMemoryStateStore(), executionStore, NewMemoryEventBus(), NewMemoryArbiter(), NewMemoryJournal())
	// No provider is registered: recovery must still reconstruct the managed
	// config before a later provider registration resumes planning.
	if err := r.recover(ctx); err != nil {
		t.Fatal(err)
	}
	r.mu.RLock()
	managed := r.configs[desired.ConfigID.Name]
	r.mu.RUnlock()
	if managed == nil {
		t.Fatal("execution-only config was not recovered")
	}
	if managed.Desired.Version != 2 || managed.Desired.Digest != "v2" || string(managed.Desired.Spec) != `{"critical":true}` {
		t.Fatalf("desired was not fully recovered: %#v", managed.Desired)
	}
	if len(managed.DependsOnConfigs) != 1 || managed.DependsOnConfigs[0] != "upstream" {
		t.Fatalf("dependencies not recovered: %#v", managed.DependsOnConfigs)
	}
	if managed.Status != model.ConfigConverging {
		t.Fatalf("status=%s, want converging", managed.Status)
	}
}

func TestPlanCloneDeepCopiesDesired(t *testing.T) {
	desired := model.DesiredState{Spec: []byte(`{"x":1}`), DependsOn: []string{"a"}}
	plan := &model.Plan{Desired: desired, Nodes: map[model.OperationKey]*model.Node{}}
	clone := plan.Clone()
	clone.Desired.Spec[0] = '['
	clone.Desired.DependsOn[0] = "changed"
	if string(plan.Desired.Spec) != `{"x":1}` || plan.Desired.DependsOn[0] != "a" {
		t.Fatalf("desired clone aliased original: %#v", plan.Desired)
	}
}
