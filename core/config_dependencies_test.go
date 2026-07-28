package core

import (
	"context"
	"testing"

	"github.com/akzj/converge/pkg/model"
)

func TestConfigDependencyCycleIsRejected(t *testing.T) {
	r := NewReconciler(NewMemoryStateStore(), NewMemoryExecutionStore(), NewMemoryEventBus(), NewMemoryArbiter(), NewMemoryJournal())
	r.configs["a"] = &model.ManagedConfig{ID: model.ConfigID{Name: "a"}, Desired: model.DesiredState{ConfigID: model.ConfigID{Name: "a"}}, DependsOnConfigs: []string{"b"}}
	candidate := model.DesiredState{ConfigID: model.ConfigID{Name: "b"}, DependsOn: []string{"a"}}
	if err := r.validateConfigDependencies(candidate); err == nil {
		t.Fatal("expected dependency cycle error")
	}
}

func TestTransitiveDependentsAreAllInvalidated(t *testing.T) {
	r := NewReconciler(NewMemoryStateStore(), NewMemoryExecutionStore(), NewMemoryEventBus(), NewMemoryArbiter(), NewMemoryJournal())
	r.configs["a"] = &model.ManagedConfig{ID: model.ConfigID{Name: "a"}}
	r.configs["b"] = &model.ManagedConfig{ID: model.ConfigID{Name: "b"}, DependsOnConfigs: []string{"a"}, Status: model.ConfigConverged}
	r.configs["c"] = &model.ManagedConfig{ID: model.ConfigID{Name: "c"}, DependsOnConfigs: []string{"b"}, Status: model.ConfigConverged}
	dependents := r.transitiveDependents("a")
	seen := make(map[string]bool)
	for _, name := range dependents {
		seen[name] = true
	}
	if !seen["b"] || !seen["c"] || len(seen) != 2 {
		t.Fatalf("transitive dependents=%#v", dependents)
	}

	// No providers are registered; invalidation still must mark every downstream
	// config non-converged before planning reports an error.
	r.invalidateDependents(context.Background(), "a")
	if r.configs["b"].Status == model.ConfigConverged || r.configs["c"].Status == model.ConfigConverged {
		t.Fatalf("downstream remained converged: b=%s c=%s", r.configs["b"].Status, r.configs["c"].Status)
	}
}
