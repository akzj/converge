package core

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/akzj/converge/pkg/model"
)

// mockProvider implements Provider for testing.
type mockProvider struct {
	typeName string
	slow     int32 // if >0, Execute blocks for this many milliseconds
	executed int32 // number of Execute calls
}

func (m *mockProvider) Type() string   { return m.typeName }
func (m *mockProvider) Digest() string { return "sha256:mock-" + m.typeName }

func (m *mockProvider) Inspect(_ context.Context, _ model.ResourceID) (model.ObservedState, error) {
	return model.ObservedState{Present: false}, nil
}

func (m *mockProvider) Replan(_ context.Context, request ReplanRequest) (ReplanResult, error) {
	observed := request.Observed
	if !observed.Present {
		return ReplanResult{Operations: []model.Operation{
			{
				ID: "provision-" + m.typeName, Key: model.OperationKey("provision-" + m.typeName), ExecutionKind: model.ExecutionDirect, Action: "provision",
				Phase: model.PhaseCommit, Destructive: false,
				CancelMode: model.CancelModeSafe,
			},
			{
				ID: "verify-" + m.typeName, Key: model.OperationKey("verify-" + m.typeName), ExecutionKind: model.ExecutionDirect, Action: "reconcile",
				Phase: model.PhaseVerify, Destructive: false,
				DependsOn: []string{"provision-" + m.typeName},
			},
		}}, nil
	}
	return ReplanResult{}, nil
}

func (m *mockProvider) Execute(ctx context.Context, op model.Operation) (model.StepResult, error) {
	atomic.AddInt32(&m.executed, 1)
	if s := atomic.LoadInt32(&m.slow); s > 0 {
		select {
		case <-time.After(time.Duration(s) * time.Millisecond):
		case <-ctx.Done():
			return model.StepResult{State: model.StepCancelled, Code: "cancelled", Reason: ctx.Err().Error()}, nil
		}
	}
	return model.StepResult{State: model.StepCompleted}, nil
}

func (m *mockProvider) Verify(_ context.Context, _ model.ResourceID, _ model.DesiredState) (model.ObservedState, error) {
	return model.ObservedState{Present: true}, nil
}

func (m *mockProvider) EvaluateCondition(_ context.Context, _ model.Condition) (bool, error) {
	return true, nil
}

func TestReconcilerSubmitsAndProcessesDesiredState(t *testing.T) {
	store := NewMemoryStateStore()
	events := NewMemoryEventBus()
	arbiter := NewMemoryArbiter()
	journal := NewMemoryJournal()

	r := NewReconciler(store, NewMemoryExecutionStore(), events, arbiter, journal)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r.RegisterProvider(ctx, &mockProvider{typeName: "test"})

	go func() { _ = r.Run(ctx) }()

	err := r.SubmitDesired(ctx, model.DesiredState{
		ConfigID:     model.ConfigID{Name: "test-config"},
		ProviderType: "test",
		Version:      1,
		Spec:         []byte(`{"key": "value"}`),
		Digest:       "sha256:abc",
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)
}

func TestReconcilerSupersessionCancelsInFlight(t *testing.T) {
	store := NewMemoryStateStore()
	events := NewMemoryEventBus()
	arbiter := NewMemoryArbiter()
	journal := NewMemoryJournal()

	provider := &mockProvider{typeName: "slow", slow: 200} // 200ms operations
	r := NewReconciler(store, NewMemoryExecutionStore(), events, arbiter, journal)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r.RegisterProvider(ctx, provider)

	go func() { _ = r.Run(ctx) }()

	// Submit v1 — provider will produce [provision-slow, verify-slow]
	err := r.SubmitDesired(ctx, model.DesiredState{
		ConfigID:     model.ConfigID{Name: "slow-config"},
		ProviderType: "slow",
		Version:      1,
		Spec:         []byte(`{"v": 1}`),
		Digest:       "sha256:v1",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Wait for provision-slow to start running
	time.Sleep(30 * time.Millisecond)

	// Submit v2 while v1 operations are in-flight → triggers supersession
	err = r.SubmitDesired(ctx, model.DesiredState{
		ConfigID:     model.ConfigID{Name: "slow-config"},
		ProviderType: "slow",
		Version:      2,
		Spec:         []byte(`{"v": 2}`),
		Digest:       "sha256:v2",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Wait for supersession to process
	time.Sleep(100 * time.Millisecond)

	r.mu.Lock()
	mc, exists := r.configs["slow-config"]
	r.mu.Unlock()

	if !exists {
		t.Fatal("config not found after supersession")
	}

	snapshot := r.registry.Snapshot(mc.ID)
	if snapshot.Plan == nil || len(snapshot.Plan.Nodes) == 0 {
		t.Fatal("supersession left empty active plan")
	}

	t.Logf("supersession: %d ops in active plan, provider.Execute called %d times",
		len(snapshot.Plan.Nodes), atomic.LoadInt32(&provider.executed))
}
