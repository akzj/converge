package core

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/akzj/converge/pkg/model"
)

func sqliteDesiredSnapshot(t *testing.T, revision uint64, items ...model.DesiredState) model.DesiredSnapshot {
	t.Helper()
	for i := range items {
		items[i].Digest = model.DesiredSpecDigest(items[i].Spec)
	}
	digest, err := model.DesiredSnapshotDigest(revision, items)
	if err != nil {
		t.Fatal(err)
	}
	return model.DesiredSnapshot{Revision: revision, Digest: digest, Items: items}
}

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

func TestSQLiteSerializesSnapshotAcceptanceWithRuntimeWrites(t *testing.T) {
	ctx := context.Background()
	store := openTestSQLite(t)
	const iterations = 100
	start := make(chan struct{})
	errs := make(chan error, 2)
	go func() {
		<-start
		for revision := uint64(1); revision <= iterations; revision++ {
			if accepted, err := store.AcceptDesiredSnapshot(ctx, sqliteDesiredSnapshot(t, revision)); err != nil || !accepted {
				errs <- errors.Join(err, errors.New("snapshot was not accepted"))
				return
			}
		}
		errs <- nil
	}()
	go func() {
		<-start
		for revision := uint64(1); revision <= iterations; revision++ {
			id := model.ConfigID{Name: "runtime"}
			if err := store.Record(ctx, model.RecordedState{ConfigID: id, ProviderType: "test", DesiredVersion: revision}); err != nil {
				errs <- err
				return
			}
			if err := store.Append(ctx, model.Event{EventID: fmt.Sprintf("event-%d", revision), ConfigID: id.Name}); err != nil {
				errs <- err
				return
			}
		}
		errs <- nil
	}()
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

func TestSQLiteDesiredSnapshotHighwaterSurvivesDeletionAndReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "snapshot.db")
	store, err := OpenSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	v5 := model.DesiredState{ConfigID: model.ConfigID{Name: "config"}, ProviderType: "test", Version: 5, Spec: []byte(`{}`)}
	if accepted, err := store.AcceptDesiredSnapshot(ctx, sqliteDesiredSnapshot(t, 1, v5)); err != nil || !accepted {
		t.Fatalf("accept v5: accepted=%v err=%v", accepted, err)
	}
	if accepted, err := store.AcceptDesiredSnapshot(ctx, sqliteDesiredSnapshot(t, 2)); err != nil || !accepted {
		t.Fatalf("accept deletion: accepted=%v err=%v", accepted, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	v4 := v5
	v4.Version = 4
	if accepted, err := reopened.AcceptDesiredSnapshot(ctx, sqliteDesiredSnapshot(t, 3, v4)); accepted || !errors.Is(err, ErrDesiredSnapshotConflict) {
		t.Fatalf("rollback after deletion: accepted=%v err=%v", accepted, err)
	}
}

func TestSQLiteSeedsDesiredHighwaterFromExistingExecution(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "upgrade.db")
	store, err := OpenSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	desired := model.DesiredState{ConfigID: model.ConfigID{Name: "config"}, ProviderType: "test", Version: 7, Spec: []byte(`{}`)}
	desired.Digest = model.DesiredSpecDigest(desired.Spec)
	if err := store.CommitExecutionCAS(ctx, desired.ConfigID, 0, ExecutionSnapshot{Revision: 1, AcceptedDesired: &desired}); err != nil {
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
	older := desired
	older.Version = 6
	if accepted, err := reopened.AcceptDesiredSnapshot(ctx, sqliteDesiredSnapshot(t, 1, older)); accepted || !errors.Is(err, ErrDesiredSnapshotConflict) {
		t.Fatalf("upgrade accepted rollback: accepted=%v err=%v", accepted, err)
	}
}

func TestSQLiteEnforcesSingleProcessOwnership(t *testing.T) {
	path := filepath.Join(t.TempDir(), "owned.db")
	first, err := OpenSQLite(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLite(context.Background(), path); err == nil {
		t.Fatal("second owner opened the same database")
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := OpenSQLite(context.Background(), path)
	if err != nil {
		t.Fatalf("database remained locked after close: %v", err)
	}
	_ = second.Close()
}

func TestSQLiteMigratesLegacyUnversionedSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE converge_state (config_id TEXT PRIMARY KEY, payload BLOB NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := OpenSQLite(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	var version int
	if err := store.db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil || version != sqliteSchemaVersion {
		t.Fatalf("schema version=%d err=%v", version, err)
	}
	if accepted, err := store.AcceptDesiredSnapshot(context.Background(), sqliteDesiredSnapshot(t, 1)); err != nil || !accepted {
		t.Fatalf("migrated snapshot store: accepted=%v err=%v", accepted, err)
	}
}

func TestSQLiteRejectsNewerSchemaAndReleasesOwnershipLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "future.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`PRAGMA user_version = 999`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSQLite(context.Background(), path); err == nil {
		t.Fatal("newer schema was accepted")
	}
	if _, err := OpenSQLite(context.Background(), path); err == nil {
		t.Fatal("newer schema was accepted after failed open")
	}
}

func TestSQLiteOnlineBackupCanBeReopened(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	store, err := OpenSQLite(ctx, filepath.Join(directory, "live.db"))
	if err != nil {
		t.Fatal(err)
	}
	snapshot := sqliteDesiredSnapshot(t, 9, model.DesiredState{ConfigID: model.ConfigID{Name: "config"}, ProviderType: "test", Version: 3, Spec: []byte(`{}`)})
	if accepted, err := store.AcceptDesiredSnapshot(ctx, snapshot); err != nil || !accepted {
		t.Fatalf("accept snapshot: accepted=%v err=%v", accepted, err)
	}
	backupPath := filepath.Join(directory, "backup.db")
	if err := store.Backup(ctx, backupPath); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	backup, err := OpenSQLite(ctx, backupPath)
	if err != nil {
		t.Fatal(err)
	}
	defer backup.Close()
	recovered, err := backup.LoadDesiredSnapshot(ctx)
	if err != nil || recovered == nil || recovered.Revision != snapshot.Revision || recovered.Digest != snapshot.Digest {
		t.Fatalf("backup snapshot=%#v err=%v", recovered, err)
	}
}

func TestSQLiteProcessKillRecoversRunningAttemptAsUnknown(t *testing.T) {
	path := os.Getenv("CONVERGE_SQLITE_CRASH_DB")
	readyPath := os.Getenv("CONVERGE_SQLITE_CRASH_READY")
	if os.Getenv("CONVERGE_SQLITE_CRASH_HELPER") == "1" {
		store, err := OpenSQLite(context.Background(), path)
		if err != nil {
			t.Fatal(err)
		}
		registry := NewPlanRegistry(store)
		plan, _, err := registry.Install(context.Background(), 0, testPlan(t, "provider", model.Operation{Key: "apply", ExecutionKind: model.ExecutionDirect}))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := registry.StartAttempt(context.Background(), plan.ConfigID, plan.Generation, "apply", "attempt-killed"); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(readyPath, []byte("ready"), 0o600); err != nil {
			t.Fatal(err)
		}
		select {}
	}

	directory := t.TempDir()
	path = filepath.Join(directory, "crash.db")
	readyPath = filepath.Join(directory, "ready")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSQLiteProcessKillRecoversRunningAttemptAsUnknown$")
	command.Env = append(os.Environ(), "CONVERGE_SQLITE_CRASH_HELPER=1", "CONVERGE_SQLITE_CRASH_DB="+path, "CONVERGE_SQLITE_CRASH_READY="+readyPath)
	var output bytes.Buffer
	command.Stdout, command.Stderr = &output, &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(readyPath); err == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = command.Process.Kill()
			_ = command.Wait()
			t.Fatalf("child did not reach durable running state: %s", output.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = command.Wait()

	store, err := OpenSQLite(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	registry := NewPlanRegistry(store)
	if err := registry.Restore(context.Background()); err != nil {
		t.Fatal(err)
	}
	snapshot := registry.Snapshot(model.ConfigID{Name: "config"})
	for _, attempt := range snapshot.Attempts {
		if attempt.ID == "attempt-killed" && attempt.Status == model.AttemptUnknown {
			return
		}
	}
	t.Fatalf("killed running attempt was not recovered as unknown: %#v", snapshot.Attempts)
}
