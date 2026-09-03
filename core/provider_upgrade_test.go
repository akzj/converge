package core

import (
	"context"
	"errors"
	"testing"

	"github.com/akzj/converge/pkg/model"
)

type versionProvider struct {
	lifecycleProvider
	digest   string
	executed chan string
}

func TestProviderVersionCanOnlyBeRemovedOfflineAndUnused(t *testing.T) {
	ctx := context.Background()
	r := NewReconciler(NewMemoryStateStore(), NewMemoryExecutionStore(), NewMemoryEventBus(), NewMemoryArbiter(), NewMemoryJournal())
	executed := make(chan string, 1)
	oldProvider := &versionProvider{lifecycleProvider: lifecycleProvider{name: "versioned"}, digest: "old", executed: executed}
	newProvider := &versionProvider{lifecycleProvider: lifecycleProvider{name: "versioned"}, digest: "new", executed: executed}
	if err := r.RegisterProviderChecked(ctx, oldProvider); err != nil {
		t.Fatal(err)
	}
	desired := model.DesiredState{ConfigID: model.ConfigID{Name: "config"}, ProviderType: oldProvider.Type(), Version: 1, Digest: "desired"}
	candidate, err := BuildCandidate(desired.ConfigID, desired, oldProvider.Type(), oldProvider.Digest(), []model.Operation{{Key: "apply", ExecutionKind: model.ExecutionDirect, Provider: oldProvider.Type()}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.registry.Install(ctx, 0, candidate); err != nil {
		t.Fatal(err)
	}
	if err := r.RegisterProviderChecked(ctx, newProvider); err != nil {
		t.Fatal(err)
	}
	if err := r.UnregisterProviderVersion(oldProvider.Type(), oldProvider.Digest()); !errors.Is(err, ErrProviderVersionInUse) {
		t.Fatalf("in-use provider removal error = %v", err)
	}

	unused := &versionProvider{lifecycleProvider: lifecycleProvider{name: "versioned"}, digest: "unused", executed: executed}
	if err := r.RegisterProviderChecked(ctx, unused); err != nil {
		t.Fatal(err)
	}
	if err := r.UnregisterProviderVersion(newProvider.Type(), newProvider.Digest()); err != nil {
		t.Fatalf("remove unused provider: %v", err)
	}
	r.mu.RLock()
	_, retained := r.providerVersions[newProvider.Type()][newProvider.Digest()]
	r.mu.RUnlock()
	if retained {
		t.Fatal("unused provider version was retained")
	}
}

func (p *versionProvider) Digest() string { return p.digest }
func (p *versionProvider) Execute(context.Context, model.Operation) (model.StepResult, error) {
	p.executed <- p.digest
	return model.StepResult{State: model.StepCompleted}, nil
}

func TestExecutorUsesProviderVersionBoundToPlanDigest(t *testing.T) {
	ctx := context.Background()
	r := NewReconciler(NewMemoryStateStore(), NewMemoryExecutionStore(), NewMemoryEventBus(), NewMemoryArbiter(), NewMemoryJournal())
	executed := make(chan string, 2)
	oldProvider := &versionProvider{lifecycleProvider: lifecycleProvider{name: "versioned"}, digest: "old", executed: executed}
	newProvider := &versionProvider{lifecycleProvider: lifecycleProvider{name: "versioned"}, digest: "new", executed: executed}
	r.RegisterProvider(ctx, oldProvider)
	desired := model.DesiredState{ConfigID: model.ConfigID{Name: "config"}, ProviderType: "versioned", Version: 1, Digest: "desired"}
	candidate, err := BuildCandidate(desired.ConfigID, desired, oldProvider.Type(), oldProvider.Digest(), []model.Operation{{Key: "apply", ExecutionKind: model.ExecutionDirect, Provider: oldProvider.Type()}})
	if err != nil {
		t.Fatal(err)
	}
	plan, _, err := r.registry.Install(ctx, 0, candidate)
	if err != nil {
		t.Fatal(err)
	}

	// Replace current provider after plan installation, before execution.
	r.RegisterProvider(ctx, newProvider)
	operation := plan.Nodes["apply"].Operation
	attempt, err := r.registry.StartAttempt(ctx, plan.ConfigID, plan.Generation, operation.Key, "attempt")
	if err != nil {
		t.Fatal(err)
	}
	r.execSem <- struct{}{}
	opCtx, cancel := context.WithCancel(ctx)
	r.mu.Lock()
	r.cancels[attempt.ID] = cancel
	r.mu.Unlock()
	r.executeAttempt(ctx, opCtx, cancel, plan, operation, attempt)
	if got := <-executed; got != "old" {
		t.Fatalf("plan executed with provider digest %q, want old", got)
	}
}
