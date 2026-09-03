package core

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/akzj/converge/pkg/model"
)

type closedEventBus struct{}

func (closedEventBus) Publish(context.Context, model.Event) error { return nil }
func (closedEventBus) Subscribe(context.Context, string) (<-chan model.Event, error) {
	events := make(chan model.Event)
	close(events)
	return events, nil
}

func TestNewReconcilerCheckedRejectsMissingDependencies(t *testing.T) {
	var typedNilStore *MemoryStateStore
	if _, err := NewReconcilerChecked(typedNilStore, NewMemoryExecutionStore(), NewMemoryEventBus(), NewMemoryArbiter(), NewMemoryJournal()); err == nil {
		t.Fatal("typed nil state store was accepted")
	}
	legacy := NewReconciler(nil, NewMemoryExecutionStore(), NewMemoryEventBus(), NewMemoryArbiter(), NewMemoryJournal())
	if err := legacy.SubmitDesired(context.Background(), model.DesiredState{}); err == nil {
		t.Fatal("legacy constructor did not report its deferred validation error")
	}
}

func TestRegisterProviderCheckedRejectsNilProvider(t *testing.T) {
	r, err := NewReconcilerChecked(NewMemoryStateStore(), NewMemoryExecutionStore(), NewMemoryEventBus(), NewMemoryArbiter(), NewMemoryJournal())
	if err != nil {
		t.Fatal(err)
	}
	var provider *mockProvider
	if err := r.RegisterProviderChecked(context.Background(), provider); err == nil {
		t.Fatal("typed nil provider was accepted")
	}
}

func TestReconcilerRejectsConcurrentRun(t *testing.T) {
	r, err := NewReconcilerChecked(NewMemoryStateStore(), NewMemoryExecutionStore(), NewMemoryEventBus(), NewMemoryArbiter(), NewMemoryJournal())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan error, 1)
	done := make(chan error, 1)
	go func() { done <- r.RunWithReady(ctx, ready) }()
	if err := <-ready; err != nil {
		t.Fatal(err)
	}
	oldProvider := &mockProvider{typeName: "old"}
	newProvider := &mockProvider{typeName: "new"}
	if err := r.RegisterProviderChecked(ctx, oldProvider); err != nil {
		t.Fatal(err)
	}
	if err := r.RegisterProviderChecked(ctx, newProvider); err != nil {
		t.Fatal(err)
	}
	if err := r.UnregisterProviderVersion(oldProvider.Type(), oldProvider.Digest()); !errors.Is(err, ErrReconcilerRunning) {
		t.Fatalf("online provider removal error = %v", err)
	}
	if err := r.Run(context.Background()); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("second Run error = %v", err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("first Run error = %v", err)
	}
	if err := r.Run(context.Background()); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("Run after shutdown error = %v", err)
	}
}

func TestReconcilerReturnsWhenEventSubscriptionCloses(t *testing.T) {
	r, err := NewReconcilerChecked(NewMemoryStateStore(), NewMemoryExecutionStore(), closedEventBus{}, NewMemoryArbiter(), NewMemoryJournal())
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); !errors.Is(err, ErrEventBusClosed) {
		t.Fatalf("Run error = %v", err)
	}
}

type drainingProvider struct {
	*mockProvider
	entered chan struct{}
	release chan struct{}
}

func (p *drainingProvider) Execute(ctx context.Context, _ model.Operation) (model.StepResult, error) {
	select {
	case <-p.entered:
	default:
		close(p.entered)
	}
	<-ctx.Done()
	<-p.release
	return model.StepResult{State: model.StepCancelled, Code: "cancelled"}, nil
}

func TestRunWaitsForTrackedProviderWorker(t *testing.T) {
	r, err := NewReconcilerChecked(NewMemoryStateStore(), NewMemoryExecutionStore(), NewMemoryEventBus(), NewMemoryArbiter(), NewMemoryJournal())
	if err != nil {
		t.Fatal(err)
	}
	provider := &drainingProvider{mockProvider: &mockProvider{typeName: "drain"}, entered: make(chan struct{}), release: make(chan struct{})}
	released := false
	defer func() {
		if !released {
			close(provider.release)
		}
	}()
	if err := r.RegisterProviderChecked(context.Background(), provider); err != nil {
		t.Fatal(err)
	}
	desired := model.DesiredState{ConfigID: model.ConfigID{Name: "config"}, ProviderType: provider.Type(), Version: 1, Spec: []byte(`{"v":1}`)}
	desired.Digest = model.DesiredSpecDigest(desired.Spec)
	if err := r.SubmitDesired(context.Background(), desired); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	select {
	case <-provider.entered:
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("provider execution did not start")
	}
	cancel()
	select {
	case err := <-done:
		t.Fatalf("Run returned before provider worker drained: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(provider.release)
	released = true
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not finish after provider worker drained")
	}
}
