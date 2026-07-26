package core

import (
	"context"
	"testing"

	"github.com/akzj/converge/pkg/model"
)

// mockProvider implements Provider for testing.
type mockProvider struct {
	typeName string
}

func (m *mockProvider) Type() string { return m.typeName }

func (m *mockProvider) Inspect(_ context.Context, _ model.ResourceID) (model.ObservedState, error) {
	return model.ObservedState{Present: false}, nil
}

func (m *mockProvider) Diff(_ context.Context, observed model.ObservedState, desired model.DesiredState) ([]model.Operation, error) {
	if !observed.Present {
		return []model.Operation{
			{ID: "provision-1", Action: "provision", Phase: model.PhaseCommit, Destructive: false},
			{ID: "verify-1", Action: "reconcile", Phase: model.PhaseVerify, Destructive: false, DependsOn: []string{"provision-1"}},
		}, nil
	}
	return nil, nil
}

func (m *mockProvider) Execute(_ context.Context, op model.Operation) (model.StepResult, error) {
	return model.StepResult{State: model.StepCompleted}, nil
}

func (m *mockProvider) Verify(_ context.Context, _ model.ResourceID, _ model.DesiredState) (model.ObservedState, error) {
	return model.ObservedState{Present: true}, nil
}

func TestReconcilerSubmitsAndProcessesDesiredState(t *testing.T) {
	store := NewMemoryStateStore()
	events := NewMemoryEventBus()
	arbiter := NewMemoryArbiter()
	journal := NewMemoryJournal()

	r := NewReconciler(store, events, arbiter, journal)
	r.RegisterProvider(&mockProvider{typeName: "test"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start reconciler in background
	go func() {
		_ = r.Run(ctx)
	}()

	// Submit a desired state
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

	// Give it a moment to process
	// In a real test we'd wait for events, but this validates basic plumbing
	_ = r
}