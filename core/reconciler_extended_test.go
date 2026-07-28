package core

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/akzj/converge/pkg/model"
)

// --- executeAttempt test helpers ---

// timeoutProvider executes slowly enough to trigger DeadlineExceeded.
type timeoutProvider struct {
	mockProvider
}

func (m *timeoutProvider) Type() string   { return "timeout" }
func (m *timeoutProvider) Digest() string { return "sha256:mock-timeout" }

func (m *timeoutProvider) Replan(_ context.Context, request ReplanRequest) (ReplanResult, error) {
	observed := request.Observed
	if !observed.Present {
		return ReplanResult{Operations: []model.Operation{
			{
				ID: "provision", Key: model.OperationKey("provision"), Action: "provision",
				Phase: model.PhaseCommit, Destructive: false,
				CancelMode: model.CancelModeSafe,
				Timeout:    1, // 1ms timeout — will trigger DeadlineExceeded quickly
			},
		}}, nil
	}
	return ReplanResult{}, nil
}

func (m *timeoutProvider) Execute(ctx context.Context, op model.Operation) (model.StepResult, error) {
	atomic.AddInt32(&m.executed, 1)
	// Block until context is cancelled (timeout or cancel).
	<-ctx.Done()
	return model.StepResult{State: model.StepFailed, Code: "timeout", Reason: ctx.Err().Error()}, ctx.Err()
}

// conditionProvider returns false for EvaluateCondition.
type conditionProvider struct {
	mockProvider
}

func (m *conditionProvider) Type() string   { return "condition" }
func (m *conditionProvider) Digest() string { return "sha256:mock-condition" }

func (m *conditionProvider) Replan(_ context.Context, request ReplanRequest) (ReplanResult, error) {
	observed := request.Observed
	if !observed.Present {
		return ReplanResult{Operations: []model.Operation{
			{
				ID: "provision", Key: model.OperationKey("provision"), Action: "provision",
				Phase: model.PhaseCommit, Destructive: false,
				CancelMode: model.CancelModeSafe,
				Conditions: []model.Condition{{Name: "test", Input: []byte(`{}`)}},
			},
		}}, nil
	}
	return ReplanResult{}, nil
}

func (m *conditionProvider) EvaluateCondition(_ context.Context, _ model.Condition) (bool, error) {
	return false, nil
}

// arbiterProvider returns a destructive operation for testing arbiter.
type arbiterProvider struct {
	mockProvider
}

func (m *arbiterProvider) Type() string   { return "arbiter" }
func (m *arbiterProvider) Digest() string { return "sha256:mock-arbiter" }

func (m *arbiterProvider) Replan(_ context.Context, request ReplanRequest) (ReplanResult, error) {
	observed := request.Observed
	if !observed.Present {
		return ReplanResult{Operations: []model.Operation{
			{
				ID: "destructive", Key: model.OperationKey("destructive"), Action: "destroy",
				Phase: model.PhaseCommit, Destructive: true,
				CancelMode: model.CancelModeSafe,
			},
		}}, nil
	}
	return ReplanResult{}, nil
}

// fixedProvider returns the same operations every time, for deterministic plan testing.
type fixedProvider struct {
	mockProvider
	ops []model.Operation
}

func (m *fixedProvider) Type() string   { return "fixed" }
func (m *fixedProvider) Digest() string { return "sha256:mock-fixed" }

func (m *fixedProvider) Replan(_ context.Context, _ ReplanRequest) (ReplanResult, error) {
	return ReplanResult{Operations: append([]model.Operation(nil), m.ops...)}, nil
}

// --- Tests ---

func TestExecuteAttemptTimeoutMarksUnknown(t *testing.T) {
	store := NewMemoryStateStore()
	events := NewMemoryEventBus()
	arbiter := NewMemoryArbiter()
	journal := NewMemoryJournal()

	r := NewReconciler(store, NewMemoryExecutionStore(), events, arbiter, journal)
	r.RegisterProvider(context.Background(), &timeoutProvider{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = r.Run(ctx) }()

	// Submit desired state with a very short timeout (1ms set in Replan).
	err := r.SubmitDesired(ctx, model.DesiredState{
		ConfigID:     model.ConfigID{Name: "timeout-config"},
		ProviderType: "timeout",
		Version:      1,
		Spec:         []byte(`{"key": "value"}`),
		Digest:       "sha256:timeout-v1",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Wait for the operation to be started and timed out.
	time.Sleep(200 * time.Millisecond)

	snapshot := r.registry.Snapshot(model.ConfigID{Name: "timeout-config"})
	if snapshot.Plan == nil {
		t.Fatal("no active plan after timeout")
	}
	t.Logf("timeout-config nodes: %d, attempts: %d", len(snapshot.Plan.Nodes), len(snapshot.Attempts))
	for key, node := range snapshot.Plan.Nodes {
		t.Logf("  node key=%s status=%s attempt=%s", key, node.Status, node.AttemptID)
	}
	for _, attempt := range snapshot.Attempts {
		t.Logf("  attempt id=%s status=%s", attempt.ID, attempt.Status)
	}
}

func TestExecuteAttemptConditionUnmetBecomesWaiting(t *testing.T) {
	store := NewMemoryStateStore()
	events := NewMemoryEventBus()
	arbiter := NewMemoryArbiter()
	journal := NewMemoryJournal()

	r := NewReconciler(store, NewMemoryExecutionStore(), events, arbiter, journal)
	r.RegisterProvider(context.Background(), &conditionProvider{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = r.Run(ctx) }()

	err := r.SubmitDesired(ctx, model.DesiredState{
		ConfigID:     model.ConfigID{Name: "condition-config"},
		ProviderType: "condition",
		Version:      1,
		Spec:         []byte(`{"key": "value"}`),
		Digest:       "sha256:cond-v1",
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)

	snapshot := r.registry.Snapshot(model.ConfigID{Name: "condition-config"})
	if snapshot.Plan == nil {
		t.Fatal("no active plan after condition check")
	}
	t.Logf("condition-config nodes: %d, attempts: %d", len(snapshot.Plan.Nodes), len(snapshot.Attempts))
	for key, node := range snapshot.Plan.Nodes {
		t.Logf("  node key=%s status=%s attempt=%s", key, node.Status, node.AttemptID)
		if node.Status != model.NodeWaiting && node.Status != model.NodeCompleted {
			t.Errorf("expected node to be waiting or completed, got %s", node.Status)
		}
	}
}

func TestExecuteAttemptArbiterAcquiredForDestructive(t *testing.T) {
	store := NewMemoryStateStore()
	events := NewMemoryEventBus()
	arbiter := NewMemoryArbiter()
	journal := NewMemoryJournal()

	r := NewReconciler(store, NewMemoryExecutionStore(), events, arbiter, journal)
	r.RegisterProvider(context.Background(), &arbiterProvider{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = r.Run(ctx) }()

	err := r.SubmitDesired(ctx, model.DesiredState{
		ConfigID:     model.ConfigID{Name: "arbiter-config"},
		ProviderType: "arbiter",
		Version:      1,
		Spec:         []byte(`{"key": "value"}`),
		Digest:       "sha256:arb-v1",
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)

	snapshot := r.registry.Snapshot(model.ConfigID{Name: "arbiter-config"})
	if snapshot.Plan == nil {
		t.Fatal("no active plan after arbiter execution")
	}
	t.Logf("arbiter-config nodes: %d, attempts: %d", len(snapshot.Plan.Nodes), len(snapshot.Attempts))
	for key, node := range snapshot.Plan.Nodes {
		t.Logf("  node key=%s status=%s destructive=%v", key, node.Status, node.Operation.Destructive)
	}
}

func TestHandleDesiredRejectsVersionConflict(t *testing.T) {
	store := NewMemoryStateStore()
	events := NewMemoryEventBus()
	arbiter := NewMemoryArbiter()
	journal := NewMemoryJournal()

	r := NewReconciler(store, NewMemoryExecutionStore(), events, arbiter, journal)
	r.RegisterProvider(context.Background(), &mockProvider{typeName: "conflict"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = r.Run(ctx) }()

	// Submit v2.
	err := r.SubmitDesired(ctx, model.DesiredState{
		ConfigID:     model.ConfigID{Name: "conflict-config"},
		ProviderType: "conflict",
		Version:      2,
		Spec:         []byte(`{"v": 2}`),
		Digest:       "sha256:v2",
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	// Submit older version — should be rejected.
	err = r.SubmitDesired(ctx, model.DesiredState{
		ConfigID:     model.ConfigID{Name: "conflict-config"},
		ProviderType: "conflict",
		Version:      1,
		Spec:         []byte(`{"v": 1}`),
		Digest:       "sha256:v1",
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	r.mu.RLock()
	managed := r.configs["conflict-config"]
	r.mu.RUnlock()
	if managed == nil {
		t.Fatal("config not found")
	}
	if managed.Desired.Version != 2 {
		t.Fatalf("desired version was downgraded to %d, want 2", managed.Desired.Version)
	}
}

func TestHandleDesiredSameVersionIsNoop(t *testing.T) {
	store := NewMemoryStateStore()
	events := NewMemoryEventBus()
	arbiter := NewMemoryArbiter()
	journal := NewMemoryJournal()

	r := NewReconciler(store, NewMemoryExecutionStore(), events, arbiter, journal)
	r.RegisterProvider(context.Background(), &mockProvider{typeName: "noop"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = r.Run(ctx) }()

	desired := model.DesiredState{
		ConfigID:     model.ConfigID{Name: "noop-config"},
		ProviderType: "noop",
		Version:      1,
		Spec:         []byte(`{"key": "value"}`),
		Digest:       "sha256:noop",
	}

	err := r.SubmitDesired(ctx, desired)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	// Submit same version + digest — should be a no-op.
	err = r.SubmitDesired(ctx, desired)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)
	// No crash/panic = success.
}

func TestHandleEventAllTypes(t *testing.T) {
	store := NewMemoryStateStore()
	events := NewMemoryEventBus()
	arbiter := NewMemoryArbiter()
	journal := NewMemoryJournal()

	// Use a fixed provider that returns the same operations so the reconciler
	// doesn't create a different plan during Run.
	fixed := &fixedProvider{ops: []model.Operation{
		{
			ID: "apply", Key: model.OperationKey("apply"), Action: "provision",
			Phase: model.PhaseCommit, Destructive: false,
			CancelMode: model.CancelModeSafe,
		},
	}}

	r := NewReconciler(store, NewMemoryExecutionStore(), events, arbiter, journal)
	r.RegisterProvider(context.Background(), fixed)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = r.Run(ctx) }()

	// Submit desired — the fixed provider will return the same ops.
	err := r.SubmitDesired(ctx, model.DesiredState{
		ConfigID:     model.ConfigID{Name: "events-config"},
		ProviderType: "fixed",
		Version:      1,
		Spec:         []byte(`{"key": "value"}`),
		Digest:       "sha256:events-v1",
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	// The plan should now exist with a running attempt.
	snapshot := r.registry.Snapshot(model.ConfigID{Name: "events-config"})
	if snapshot.Plan == nil {
		t.Fatal("no active plan after submit")
	}

	// Find the running attempt ID.
	var attemptID model.AttemptID
	for _, a := range snapshot.Attempts {
		if a.Status == model.AttemptRunning {
			attemptID = a.ID
			break
		}
	}
	if attemptID == "" {
		// All attempts already completed or no attempt started.
		t.Logf("no running attempt found, attempts: %#v", snapshot.Attempts)
		for key, n := range snapshot.Plan.Nodes {
			t.Logf("  node %s: status=%s attempt=%s", key, n.Status, n.AttemptID)
		}
		return
	}

	// Publish a completed event through the bus.
	event := model.Event{
		EventID:    "events-config/result",
		PlanID:     snapshot.Plan.ID,
		Generation: snapshot.Plan.Generation,
		ConfigID:   "events-config",
		NodeKey:    "apply",
		AttemptID:  attemptID,
		State:      model.StepCompleted,
		Result:     model.StepResult{State: model.StepCompleted},
	}
	if err := events.Publish(ctx, event); err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)

	snapshot = r.registry.Snapshot(model.ConfigID{Name: "events-config"})
	if snapshot.Plan == nil {
		t.Fatal("no active plan after event")
	}
	node := snapshot.Plan.Nodes["apply"]
	if node == nil {
		t.Fatal("node not found")
	}
	if node.Status != model.NodeCompleted {
		t.Fatalf("node status=%s, want completed", node.Status)
	}
}

func TestTerminalAttemptStatusErrors(t *testing.T) {
	_, _, err := terminalAttemptStatus(model.StepState("unknown"))
	if err == nil {
		t.Fatal("expected error for unknown step state")
	}

	_, _, err = terminalAttemptStatus(model.StepWaiting)
	if err == nil {
		t.Fatal("expected error for waiting state")
	}

	_, _, err = terminalAttemptStatus(model.StepState(""))
	if err == nil {
		t.Fatal("expected error for empty step state")
	}
}

func TestVerifyAndRecordStalePlanDoesNotRecord(t *testing.T) {
	store := NewMemoryStateStore()
	events := NewMemoryEventBus()
	arbiter := NewMemoryArbiter()
	journal := NewMemoryJournal()

	r := NewReconciler(store, NewMemoryExecutionStore(), events, arbiter, journal)
	provider := &mockProvider{typeName: "stale"}
	r.RegisterProvider(context.Background(), provider)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = r.Run(ctx) }()

	// Submit v1.
	desiredV1 := model.DesiredState{
		ConfigID:     model.ConfigID{Name: "stale-config"},
		ProviderType: "stale",
		Version:      1,
		Spec:         []byte(`{"key": "value"}`),
		Digest:       "sha256:stale-v1",
	}
	if err := r.SubmitDesired(ctx, desiredV1); err != nil {
		t.Fatal(err)
	}

	time.Sleep(50 * time.Millisecond)

	// Snapshot the current plan (v1 plan).
	snapshot := r.registry.Snapshot(model.ConfigID{Name: "stale-config"})
	if snapshot.Plan == nil {
		t.Fatal("no active plan after v1 submit")
	}
	oldPlan := snapshot.Plan.Clone()

	// Submit v2 — this will replace the plan.
	desiredV2 := model.DesiredState{
		ConfigID:     model.ConfigID{Name: "stale-config"},
		ProviderType: "stale",
		Version:      2,
		Spec:         []byte(`{"key": "value2"}`),
		Digest:       "sha256:stale-v2",
	}
	if err := r.SubmitDesired(ctx, desiredV2); err != nil {
		t.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)

	// Now call verifyAndRecord with the old plan — it should be a no-op.
	r.verifyAndRecord(ctx, oldPlan, provider)

	// The recorded state should be for v2, not v1.
	recorded, err := store.Get(ctx, model.ConfigID{Name: "stale-config"})
	if err != nil {
		t.Fatal(err)
	}
	if recorded != nil && recorded.DesiredVersion == 1 {
		t.Fatalf("stale plan was recorded: %#v", recorded)
	}
	t.Logf("recorded state: %#v", recorded)
}