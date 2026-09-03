package edge

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/akzj/converge/core"
	"github.com/akzj/converge/pkg/model"
)

type noOpProvider struct{}

type failingSnapshotStore struct{ err error }

func (s failingSnapshotStore) AcceptDesiredSnapshot(context.Context, model.DesiredSnapshot) (bool, error) {
	return false, s.err
}
func (s failingSnapshotStore) LoadDesiredSnapshot(context.Context) (*model.DesiredSnapshot, error) {
	return nil, s.err
}

func (noOpProvider) Type() string   { return "noop" }
func (noOpProvider) Digest() string { return "sha256:noop" }
func (noOpProvider) Inspect(context.Context, model.ResourceID) (model.ObservedState, error) {
	return model.ObservedState{}, nil
}
func (noOpProvider) Replan(context.Context, core.ReplanRequest) (core.ReplanResult, error) {
	return core.ReplanResult{}, nil
}
func (noOpProvider) EvaluateCondition(context.Context, model.Condition) (bool, error) {
	return true, nil
}
func (noOpProvider) Execute(context.Context, model.Operation) (model.StepResult, error) {
	return model.StepResult{State: model.StepCompleted}, nil
}
func (noOpProvider) Verify(_ context.Context, resource model.ResourceID, desired model.DesiredState) (model.ObservedState, error) {
	return model.ObservedState{ConfigID: desired.ConfigID, Version: "ready", Present: true}, nil
}

func desired(name string, version uint64, dependencies ...string) model.DesiredState {
	item := model.DesiredState{ConfigID: model.ConfigID{Name: name}, ProviderType: "noop", Version: version, Spec: []byte(`{}`), DependsOn: dependencies}
	item.Digest = model.DesiredSpecDigest(item.Spec)
	return item
}

func snapshot(t *testing.T, revision uint64, items ...model.DesiredState) model.DesiredSnapshot {
	t.Helper()
	digest, err := model.DesiredSnapshotDigest(revision, items)
	if err != nil {
		t.Fatal(err)
	}
	return model.DesiredSnapshot{Revision: revision, Digest: digest, Items: items}
}

func newTestRuntime(t *testing.T) (*Runtime, *core.Reconciler, *core.MemoryDesiredSnapshotStore) {
	t.Helper()
	store := core.NewMemoryDesiredSnapshotStore()
	reconciler := core.NewReconciler(core.NewMemoryStateStore(), core.NewMemoryExecutionStore(), core.NewMemoryEventBus(), core.NewMemoryArbiter(), core.NewMemoryJournal())
	reconciler.RegisterProvider(context.Background(), noOpProvider{})
	runtime, err := NewRuntime(reconciler, store)
	if err != nil {
		t.Fatal(err)
	}
	return runtime, reconciler, store
}

func TestRuntimeDurableAckReplayAndMissingDeletion(t *testing.T) {
	runtime, reconciler, _ := newTestRuntime(t)
	first := snapshot(t, 1, desired("a", 1), desired("b", 1, "a"))
	if ack := runtime.SubmitSnapshot(context.Background(), first); !ack.Accepted || ack.Code != "accepted" {
		t.Fatalf("first ack=%#v", ack)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	waitFor(t, func() bool {
		a, aOK := reconciler.Config("a")
		b, bOK := reconciler.Config("b")
		return aOK && bOK && a.Status == model.ConfigConverged && b.Status == model.ConfigConverged
	})

	second := snapshot(t, 2, desired("a", 2))
	if ack := runtime.SubmitSnapshot(context.Background(), second); !ack.Accepted {
		t.Fatalf("second ack=%#v", ack)
	}
	waitFor(t, func() bool {
		_, bOK := reconciler.Config("b")
		status, _ := runtime.Status(context.Background())
		return !bOK && status.DispatchedRevision == 2
	})
	cancel()
	<-done
}

func TestRuntimeRejectsPerConfigVersionRollbackAfterDeletion(t *testing.T) {
	runtime, _, _ := newTestRuntime(t)
	for _, current := range []model.DesiredSnapshot{
		snapshot(t, 1, desired("a", 5)),
		snapshot(t, 2),
	} {
		if ack := runtime.SubmitSnapshot(context.Background(), current); !ack.Accepted {
			t.Fatalf("ack=%#v", ack)
		}
	}
	ack := runtime.SubmitSnapshot(context.Background(), snapshot(t, 3, desired("a", 4)))
	if ack.Accepted || ack.Code != "revision_conflict" {
		t.Fatalf("rollback ack=%#v", ack)
	}
}

func TestRuntimeDoesNotAckPersistenceFailure(t *testing.T) {
	reconciler := core.NewReconciler(core.NewMemoryStateStore(), core.NewMemoryExecutionStore(), core.NewMemoryEventBus(), core.NewMemoryArbiter(), core.NewMemoryJournal())
	runtime, err := NewRuntime(reconciler, failingSnapshotStore{err: context.DeadlineExceeded})
	if err != nil {
		t.Fatal(err)
	}
	ack := runtime.SubmitSnapshot(context.Background(), snapshot(t, 1))
	if ack.Accepted || ack.Code != "persistence_failed" {
		t.Fatalf("persistence failure ack=%#v", ack)
	}
}

func TestRuntimeReplaysAcceptedSnapshotAfterRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "edge.db")
	store, err := core.OpenSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	reconciler := core.NewReconciler(store, store, core.NewMemoryEventBus(), core.NewMemoryArbiter(), store)
	reconciler.RegisterProvider(ctx, noOpProvider{})
	runtime, _ := NewRuntime(reconciler, store)
	first := snapshot(t, 1, desired("a", 1), desired("b", 1))
	if ack := runtime.SubmitSnapshot(ctx, first); !ack.Accepted {
		t.Fatalf("first ack=%#v", ack)
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- runtime.Run(runCtx) }()
	waitFor(t, func() bool { return len(reconciler.ConfigNames()) == 2 })
	cancel()
	<-done
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	// Persist revision 2 but simulate process loss before Runtime.Run can
	// dispatch its missing-item deletion to Core.
	store, err = core.OpenSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	second := snapshot(t, 2, desired("a", 2))
	if accepted, err := store.AcceptDesiredSnapshot(ctx, second); err != nil || !accepted {
		t.Fatalf("second acceptance: accepted=%v err=%v", accepted, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = core.OpenSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	reconciler = core.NewReconciler(store, store, core.NewMemoryEventBus(), core.NewMemoryArbiter(), store)
	reconciler.RegisterProvider(ctx, noOpProvider{})
	runtime, _ = NewRuntime(reconciler, store)
	runCtx, cancel = context.WithCancel(ctx)
	done = make(chan error, 1)
	go func() { done <- runtime.Run(runCtx) }()
	waitFor(t, func() bool {
		_, bExists := reconciler.Config("b")
		status, statusErr := runtime.Status(ctx)
		return statusErr == nil && status.DispatchedRevision == 2 && !bExists
	})
	cancel()
	<-done
}

func TestHTTPHandlerAuthenticationAndNegotiation(t *testing.T) {
	runtime, _, _ := newTestRuntime(t)
	handler, err := NewHTTPHandler(runtime, "secret")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()

	response, err := http.Get(server.URL + "/v1/status")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status=%d", response.StatusCode)
	}

	desiredSnapshot := snapshot(t, 1, desired("a", 1))
	body, err := json.Marshal(desiredSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/desired-snapshots", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Content-Type", "application/json")
	response, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("snapshot status=%d", response.StatusCode)
	}
	// A retry after a lost ACK is idempotently acknowledged.
	req, _ = http.NewRequest(http.MethodPost, server.URL+"/v1/desired-snapshots", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer secret")
	response, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var ack SnapshotACK
	if err := json.NewDecoder(response.Body).Decode(&ack); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK || !ack.Accepted || ack.Code != "duplicate" {
		t.Fatalf("duplicate status=%d ack=%#v", response.StatusCode, ack)
	}

	req, _ = http.NewRequest(http.MethodGet, server.URL+"/v1/desired-snapshots/current", nil)
	req.Header.Set("Authorization", "Bearer secret")
	response, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("negotiation status=%d", response.StatusCode)
	}
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition was not satisfied before deadline")
}
