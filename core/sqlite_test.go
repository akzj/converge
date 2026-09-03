package core

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/akzj/converge/pkg/model"
)

func openTestSQLite(t *testing.T) *SQLiteStore {
	t.Helper()
	store, err := OpenSQLite(context.Background(), filepath.Join(t.TempDir(), "converge.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestSQLiteStoreStateExecutionCASAndJournal(t *testing.T) {
	ctx := context.Background()
	store := openTestSQLite(t)
	id := model.ConfigID{Name: "config"}
	recorded := model.RecordedState{ConfigID: id, ProviderType: "test", DesiredVersion: 1, DesiredDigest: "digest", Status: model.ConfigConverged}
	if err := store.Record(ctx, recorded); err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(ctx, id)
	if err != nil || got == nil || got.DesiredVersion != 1 || got.UpdatedAt.IsZero() {
		t.Fatalf("recorded state=%#v err=%v", got, err)
	}

	desired := model.DesiredState{ConfigID: id, ProviderType: "test", Version: 2, Spec: []byte(`{"v":2}`)}
	desired.Digest = model.DesiredSpecDigest(desired.Spec)
	first := ExecutionSnapshot{Revision: 1, AcceptedDesired: &desired}
	if err := store.CommitExecutionCAS(ctx, id, 0, first); err != nil {
		t.Fatal(err)
	}
	if err := store.CommitExecutionCAS(ctx, id, 0, first); !errors.Is(err, ErrGenerationChanged) {
		t.Fatalf("stale insert err=%v", err)
	}
	second := first
	second.Revision = 2
	if err := store.CommitExecutionCAS(ctx, id, 1, second); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadExecution(ctx, id)
	if err != nil || loaded == nil || loaded.Revision != 2 || loaded.AcceptedDesired == nil {
		t.Fatalf("execution=%#v err=%v", loaded, err)
	}

	event := model.Event{EventID: "event-1", ConfigID: id.Name, State: model.StepCompleted}
	if err := store.Append(ctx, event); err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, event); err != nil {
		t.Fatal(err)
	}
	events, err := store.Events(ctx, id.Name)
	if err != nil || len(events) != 1 || events[0].EventID != event.EventID {
		t.Fatalf("events=%#v err=%v", events, err)
	}
}

func TestSQLiteStoreReopenRecoversAcceptedDesiredBeforePlan(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "recovery.db")
	store, err := OpenSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	desired := model.DesiredState{ConfigID: model.ConfigID{Name: "accepted"}, ProviderType: "missing", Version: 3, Spec: []byte(`{"v":3}`)}
	desired.Digest = model.DesiredSpecDigest(desired.Spec)
	r := NewReconciler(store, store, NewMemoryEventBus(), NewMemoryArbiter(), store)
	if err := r.SubmitDesired(ctx, desired); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	recovered := NewReconciler(reopened, reopened, NewMemoryEventBus(), NewMemoryArbiter(), reopened)
	if err := recovered.recover(ctx); err != nil {
		t.Fatal(err)
	}
	config, ok := recovered.Config(desired.ConfigID.Name)
	if !ok || config.Desired.Version != desired.Version || config.Desired.Digest != desired.Digest {
		t.Fatalf("recovered config=%#v", config)
	}
}

func TestSQLiteExecutionCASAllowsIndependentConfigs(t *testing.T) {
	ctx := context.Background()
	store := openTestSQLite(t)
	start := make(chan struct{})
	errs := make(chan error, 2)
	for _, name := range []string{"a", "b"} {
		go func(name string) {
			<-start
			errs <- store.CommitExecutionCAS(ctx, model.ConfigID{Name: name}, 0, ExecutionSnapshot{Revision: 1})
		}(name)
	}
	close(start)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
}

func TestSQLiteReopenRecoversRunningAttemptAsUnknown(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "attempt-recovery.db")
	store, err := OpenSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	registry := NewPlanRegistry(store)
	plan, _, err := registry.Install(ctx, 0, testPlan(t, "provider", model.Operation{Key: "apply", ExecutionKind: model.ExecutionDirect}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.StartAttempt(ctx, plan.ConfigID, plan.Generation, "apply", "attempt-1"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	recovered := NewPlanRegistry(reopened)
	if err := recovered.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot := recovered.Snapshot(plan.ConfigID)
	if snapshot.Plan == nil || snapshot.Plan.Nodes["apply"].Status != model.NodeDraining {
		t.Fatalf("running node was not recovered conservatively: %#v", snapshot.Plan)
	}
	var foundUnknown bool
	for _, attempt := range snapshot.Attempts {
		if attempt.ID == "attempt-1" && attempt.Status == model.AttemptUnknown {
			foundUnknown = true
		}
	}
	if !foundUnknown {
		t.Fatalf("running attempt was not recovered as unknown: %#v", snapshot.Attempts)
	}
}
