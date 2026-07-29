package core

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/akzj/converge/pkg/model"
)

type lifecycleProvider struct {
	name                string
	conditionFalseCount int32
	retryFailures       int32
	conditionCalls      atomic.Int32
	executeCalls        atomic.Int32
}

func (p *lifecycleProvider) Type() string   { return p.name }
func (p *lifecycleProvider) Digest() string { return "digest-" + p.name }
func (p *lifecycleProvider) Inspect(context.Context, model.ResourceID) (model.ObservedState, error) {
	return model.ObservedState{Present: false}, nil
}
func (p *lifecycleProvider) Replan(context.Context, ReplanRequest) (ReplanResult, error) {
	return ReplanResult{Operations: []model.Operation{{Key: "apply", ExecutionKind: model.ExecutionDirect, Action: "apply", Conditions: []model.Condition{{Name: "ready"}}}}}, nil
}
func (p *lifecycleProvider) EvaluateCondition(context.Context, model.Condition) (bool, error) {
	call := p.conditionCalls.Add(1)
	return call > p.conditionFalseCount, nil
}
func (p *lifecycleProvider) Execute(context.Context, model.Operation) (model.StepResult, error) {
	call := p.executeCalls.Add(1)
	if call <= p.retryFailures {
		return model.StepResult{State: model.StepFailed, Retryable: true, Code: "temporary"}, nil
	}
	return model.StepResult{State: model.StepCompleted}, nil
}
func (p *lifecycleProvider) Verify(context.Context, model.ResourceID, model.DesiredState) (model.ObservedState, error) {
	return model.ObservedState{Present: true}, nil
}

func startTestReconciler(t *testing.T, provider Provider) (*Reconciler, context.Context) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	r := NewReconciler(NewMemoryStateStore(), NewMemoryExecutionStore(), NewMemoryEventBus(), NewMemoryArbiter(), NewMemoryJournal())
	r.RegisterProvider(ctx, provider)
	go func() { _ = r.Run(ctx) }()
	return r, ctx
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not reached before timeout")
}

func TestReconcilerConditionWaitsThenExecutesEndToEnd(t *testing.T) {
	provider := &lifecycleProvider{name: "condition", conditionFalseCount: 1}
	r, ctx := startTestReconciler(t, provider)
	desired := model.DesiredState{ConfigID: model.ConfigID{Name: "condition-config"}, ProviderType: provider.Type(), Version: 1, Digest: "v1"}
	if err := r.SubmitDesired(ctx, desired); err != nil {
		t.Fatal(err)
	}

	waitFor(t, func() bool {
		snapshot := r.registry.Snapshot(desired.ConfigID)
		return snapshot.Plan != nil && snapshot.Plan.Nodes["apply"].Status == model.NodeWaiting
	})
	if provider.executeCalls.Load() != 0 {
		t.Fatalf("execute ran while condition unmet: %d", provider.executeCalls.Load())
	}
	if err := r.registry.WakeDueWaiting(ctx, time.Now().Add(10*time.Second)); err != nil {
		t.Fatal(err)
	}
	r.executeReady(ctx)
	waitFor(t, func() bool {
		r.mu.RLock()
		defer r.mu.RUnlock()
		return r.configs[desired.ConfigID.Name].Status == model.ConfigConverged
	})
	if provider.conditionCalls.Load() != 2 || provider.executeCalls.Load() != 1 {
		t.Fatalf("calls: condition=%d execute=%d", provider.conditionCalls.Load(), provider.executeCalls.Load())
	}
}

func TestReconcilerRetriesWithFreshAttemptsEndToEnd(t *testing.T) {
	provider := &lifecycleProvider{name: "retry", retryFailures: 2}
	r, ctx := startTestReconciler(t, provider)
	desired := model.DesiredState{ConfigID: model.ConfigID{Name: "retry-config"}, ProviderType: provider.Type(), Version: 1, Digest: "v1"}
	if err := r.SubmitDesired(ctx, desired); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		r.mu.RLock()
		defer r.mu.RUnlock()
		config := r.configs[desired.ConfigID.Name]
		return config != nil && config.Status == model.ConfigConverged
	})
	if provider.executeCalls.Load() != 3 {
		t.Fatalf("execute calls=%d, want 3", provider.executeCalls.Load())
	}
	snapshot := r.registry.Snapshot(desired.ConfigID)
	if snapshot.Plan.Nodes["apply"].RetryCount != 2 {
		t.Fatalf("retry count=%d, want 2", snapshot.Plan.Nodes["apply"].RetryCount)
	}
	seen := make(map[model.AttemptID]bool)
	for _, attempt := range snapshot.Attempts {
		seen[attempt.ID] = true
	}
	if len(seen) != 3 {
		t.Fatalf("unique attempts=%d, want 3: %#v", len(seen), snapshot.Attempts)
	}
}
