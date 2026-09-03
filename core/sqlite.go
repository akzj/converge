package core

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"

	"github.com/akzj/converge/pkg/model"
)

// SQLiteStore is a durable implementation of StateStore, ExecutionStore, and
// Journal. Execution updates use a revision-guarded statement, so competing
// writers cannot silently overwrite a newer snapshot.
type SQLiteStore struct {
	db         *sql.DB
	lockFile   *os.File
	snapshotMu sync.Mutex
	path       string
}

const sqliteSchemaVersion = 2

// OpenSQLite opens or creates a SQLite database suitable for an embedded runtime.
// WAL permits readers and writers to make progress concurrently; busy_timeout
// turns short writer contention into bounded waiting instead of immediate loss.
func OpenSQLite(ctx context.Context, path string) (*SQLiteStore, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("sqlite path is empty")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, errors.Wrap(err, "resolve sqlite path")
	}
	lockFile, err := os.OpenFile(abs+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, errors.Wrap(err, "open sqlite ownership lock")
	}
	if err := unix.Flock(int(lockFile.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = lockFile.Close()
		return nil, errors.Wrap(err, "sqlite database is already owned by another process")
	}
	dsn := (&url.URL{Scheme: "file", Path: abs, RawQuery: "_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)"}).String()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		_ = unix.Flock(int(lockFile.Fd()), unix.LOCK_UN)
		_ = lockFile.Close()
		return nil, errors.Wrap(err, "open sqlite")
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)
	store := &SQLiteStore{db: db, lockFile: lockFile, path: abs}
	if err := store.init(ctx); err != nil {
		_ = store.Close()
		return nil, err
	}
	if err := os.Chmod(abs, 0o600); err != nil {
		_ = store.Close()
		return nil, errors.Wrap(err, "secure sqlite database permissions")
	}
	return store, nil
}

func (s *SQLiteStore) init(ctx context.Context) error {
	var version int
	if err := s.db.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&version); err != nil {
		return errors.Wrap(err, "read sqlite schema version")
	}
	if version > sqliteSchemaVersion {
		return errors.Errorf("sqlite schema version %d is newer than supported version %d", version, sqliteSchemaVersion)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "begin sqlite migration")
	}
	defer func() { _ = tx.Rollback() }()
	if version < 1 {
		const schemaV1 = `
CREATE TABLE IF NOT EXISTS converge_state (
    config_id TEXT PRIMARY KEY,
    payload BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS converge_execution (
    config_id TEXT PRIMARY KEY,
    revision INTEGER NOT NULL,
    payload BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS converge_journal (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    config_id TEXT NOT NULL,
    event_key TEXT UNIQUE,
    payload BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS converge_journal_config_sequence
    ON converge_journal(config_id, sequence);`
		if _, err := tx.ExecContext(ctx, schemaV1); err != nil {
			return errors.Wrap(err, "apply sqlite schema v1")
		}
	}
	if version < 2 {
		const schemaV2 = `
CREATE TABLE IF NOT EXISTS converge_desired_snapshot (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    revision INTEGER NOT NULL,
    digest TEXT NOT NULL,
    payload BLOB NOT NULL,
    accepted_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS converge_desired_highwater (
    config_id TEXT PRIMARY KEY,
    version INTEGER NOT NULL,
    identity_digest TEXT NOT NULL
);`
		if _, err := tx.ExecContext(ctx, schemaV2); err != nil {
			return errors.Wrap(err, "apply sqlite schema v2")
		}
	}
	if _, err := tx.ExecContext(ctx, `PRAGMA user_version = 2`); err != nil {
		return errors.Wrap(err, "record sqlite schema version")
	}
	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "commit sqlite migration")
	}
	return s.seedDesiredHighwater(ctx)
}

func (s *SQLiteStore) seedDesiredHighwater(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `SELECT payload FROM converge_execution`)
	if err != nil {
		return errors.Wrap(err, "scan execution state for desired high-water marks")
	}
	defer rows.Close()
	type mark struct {
		name, digest string
		version      uint64
	}
	var marks []mark
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return errors.Wrap(err, "scan execution for desired high-water mark")
		}
		var snapshot ExecutionSnapshot
		if err := json.Unmarshal(payload, &snapshot); err != nil {
			return errors.Wrap(err, "decode execution for desired high-water mark")
		}
		var desired *model.DesiredState
		if snapshot.AcceptedDesired != nil {
			desired = snapshot.AcceptedDesired
		} else if snapshot.Plan != nil {
			desired = &snapshot.Plan.Desired
		}
		if desired == nil {
			continue
		}
		digest, err := model.DesiredStateIdentityDigest(*desired)
		if err != nil {
			return errors.Wrap(err, "digest execution desired high-water mark")
		}
		marks = append(marks, mark{name: desired.ConfigID.Name, version: desired.Version, digest: digest})
	}
	if err := rows.Err(); err != nil {
		return errors.Wrap(err, "iterate execution desired high-water marks")
	}
	for _, current := range marks {
		if _, err := s.db.ExecContext(ctx, `INSERT INTO converge_desired_highwater(config_id, version, identity_digest) VALUES(?, ?, ?)
ON CONFLICT(config_id) DO UPDATE SET version = excluded.version, identity_digest = excluded.identity_digest
WHERE converge_desired_highwater.version < excluded.version`, current.name, current.version, current.digest); err != nil {
			return errors.Wrap(err, "seed desired high-water mark")
		}
	}
	return nil
}

func (s *SQLiteStore) Close() error {
	dbErr := s.db.Close()
	unlockErr := unix.Flock(int(s.lockFile.Fd()), unix.LOCK_UN)
	closeErr := s.lockFile.Close()
	return errors.CombineErrors(dbErr, errors.CombineErrors(unlockErr, closeErr))
}

// Ping verifies that the database is reachable within the caller's deadline.
func (s *SQLiteStore) Ping(ctx context.Context) error {
	return errors.Wrap(s.db.PingContext(ctx), "ping sqlite")
}

// Backup creates a transactionally consistent standalone database using
// SQLite's online VACUUM INTO facility. The destination must not already exist.
func (s *SQLiteStore) Backup(ctx context.Context, destination string) error {
	abs, err := filepath.Abs(destination)
	if err != nil {
		return errors.Wrap(err, "resolve sqlite backup path")
	}
	if abs == s.path {
		return errors.New("sqlite backup destination is the live database")
	}
	if _, err := os.Stat(abs); err == nil {
		return errors.New("sqlite backup destination already exists")
	} else if !os.IsNotExist(err) {
		return errors.Wrap(err, "inspect sqlite backup destination")
	}
	if _, err := s.db.ExecContext(ctx, `VACUUM INTO ?`, abs); err != nil {
		return errors.Wrap(err, "backup sqlite")
	}
	return errors.Wrap(os.Chmod(abs, 0o600), "secure sqlite backup permissions")
}

func (s *SQLiteStore) AcceptDesiredSnapshot(ctx context.Context, snapshot model.DesiredSnapshot) (bool, error) {
	if err := model.ValidateDesiredSnapshot(snapshot); err != nil {
		return false, err
	}
	s.snapshotMu.Lock()
	defer s.snapshotMu.Unlock()
	payload, err := json.Marshal(snapshot)
	if err != nil {
		return false, errors.Wrap(err, "encode desired snapshot")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, errors.Wrap(err, "begin desired snapshot acceptance")
	}
	defer func() { _ = tx.Rollback() }()
	var currentRevision uint64
	var currentDigest string
	err = tx.QueryRowContext(ctx, `SELECT revision, digest FROM converge_desired_snapshot WHERE singleton = 1`).Scan(&currentRevision, &currentDigest)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, errors.Wrap(err, "load current desired snapshot identity")
	}
	if err == nil {
		if snapshot.Revision < currentRevision || snapshot.Revision == currentRevision && snapshot.Digest != currentDigest {
			return false, ErrDesiredSnapshotConflict
		}
		if snapshot.Revision == currentRevision {
			return false, nil
		}
	}
	identities := make(map[string]string, len(snapshot.Items))
	for _, desired := range snapshot.Items {
		identity, err := model.DesiredStateIdentityDigest(desired)
		if err != nil {
			return false, err
		}
		identities[desired.ConfigID.Name] = identity
		var previousVersion uint64
		var previousDigest string
		err = tx.QueryRowContext(ctx, `SELECT version, identity_digest FROM converge_desired_highwater WHERE config_id = ?`, desired.ConfigID.Name).Scan(&previousVersion, &previousDigest)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return false, errors.Wrap(err, "load desired version high-water mark")
		}
		if err == nil && (desired.Version < previousVersion || desired.Version == previousVersion && identity != previousDigest) {
			return false, ErrDesiredSnapshotConflict
		}
	}
	result, err := tx.ExecContext(ctx, `INSERT INTO converge_desired_snapshot(singleton, revision, digest, payload, accepted_at)
VALUES(1, ?, ?, ?, ?) ON CONFLICT(singleton) DO UPDATE SET
revision = excluded.revision, digest = excluded.digest, payload = excluded.payload, accepted_at = excluded.accepted_at
WHERE converge_desired_snapshot.revision < excluded.revision`, snapshot.Revision, snapshot.Digest, payload, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return false, errors.Wrap(err, "accept desired snapshot")
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, errors.Wrap(err, "read desired snapshot acceptance")
	}
	if rows == 1 {
		for _, desired := range snapshot.Items {
			if _, err := tx.ExecContext(ctx, `INSERT INTO converge_desired_highwater(config_id, version, identity_digest) VALUES(?, ?, ?)
ON CONFLICT(config_id) DO UPDATE SET version = excluded.version, identity_digest = excluded.identity_digest
WHERE converge_desired_highwater.version < excluded.version`, desired.ConfigID.Name, desired.Version, identities[desired.ConfigID.Name]); err != nil {
				return false, errors.Wrap(err, "update desired version high-water mark")
			}
		}
		if err := tx.Commit(); err != nil {
			return false, errors.Wrap(err, "commit desired snapshot acceptance")
		}
		return true, nil
	}
	return false, ErrDesiredSnapshotConflict
}

func (s *SQLiteStore) LoadDesiredSnapshot(ctx context.Context) (*model.DesiredSnapshot, error) {
	var payload []byte
	err := s.db.QueryRowContext(ctx, `SELECT payload FROM converge_desired_snapshot WHERE singleton = 1`).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, errors.Wrap(err, "load desired snapshot")
	}
	var snapshot model.DesiredSnapshot
	if err := json.Unmarshal(payload, &snapshot); err != nil {
		return nil, errors.Wrap(err, "decode desired snapshot")
	}
	return &snapshot, nil
}

func (s *SQLiteStore) Get(ctx context.Context, id model.ConfigID) (*model.RecordedState, error) {
	var payload []byte
	err := s.db.QueryRowContext(ctx, `SELECT payload FROM converge_state WHERE config_id = ?`, id.Name).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, errors.Wrap(err, "load recorded state")
	}
	var state model.RecordedState
	if err := json.Unmarshal(payload, &state); err != nil {
		return nil, errors.Wrap(err, "decode recorded state")
	}
	return &state, nil
}

func (s *SQLiteStore) List(ctx context.Context) ([]model.ConfigID, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT config_id FROM converge_state ORDER BY config_id`)
	if err != nil {
		return nil, errors.Wrap(err, "list recorded states")
	}
	defer rows.Close()
	var ids []model.ConfigID
	for rows.Next() {
		var id model.ConfigID
		if err := rows.Scan(&id.Name); err != nil {
			return nil, errors.Wrap(err, "scan recorded state ID")
		}
		ids = append(ids, id)
	}
	return ids, errors.Wrap(rows.Err(), "iterate recorded states")
}

func (s *SQLiteStore) Record(ctx context.Context, state model.RecordedState) error {
	state.UpdatedAt = time.Now()
	payload, err := json.Marshal(state)
	if err != nil {
		return errors.Wrap(err, "encode recorded state")
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO converge_state(config_id, payload) VALUES(?, ?)
ON CONFLICT(config_id) DO UPDATE SET payload = excluded.payload`, state.ConfigID.Name, payload)
	return errors.Wrap(err, "store recorded state")
}

func (s *SQLiteStore) Delete(ctx context.Context, id model.ConfigID) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM converge_state WHERE config_id = ?`, id.Name)
	return errors.Wrap(err, "delete recorded state")
}

func (s *SQLiteStore) LoadExecution(ctx context.Context, id model.ConfigID) (*ExecutionSnapshot, error) {
	var payload []byte
	err := s.db.QueryRowContext(ctx, `SELECT payload FROM converge_execution WHERE config_id = ?`, id.Name).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, errors.Wrap(err, "load execution")
	}
	var snapshot ExecutionSnapshot
	if err := json.Unmarshal(payload, &snapshot); err != nil {
		return nil, errors.Wrap(err, "decode execution")
	}
	return &snapshot, nil
}

func (s *SQLiteStore) ListExecutions(ctx context.Context) ([]model.ConfigID, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT config_id FROM converge_execution ORDER BY config_id`)
	if err != nil {
		return nil, errors.Wrap(err, "list executions")
	}
	defer rows.Close()
	var ids []model.ConfigID
	for rows.Next() {
		var id model.ConfigID
		if err := rows.Scan(&id.Name); err != nil {
			return nil, errors.Wrap(err, "scan execution ID")
		}
		ids = append(ids, id)
	}
	return ids, errors.Wrap(rows.Err(), "iterate executions")
}

func (s *SQLiteStore) CommitExecutionCAS(ctx context.Context, id model.ConfigID, expectedRevision uint64, snapshot ExecutionSnapshot) error {
	if snapshot.Revision != expectedRevision+1 {
		return errors.Errorf("invalid execution revision %d, want %d", snapshot.Revision, expectedRevision+1)
	}
	payload, err := json.Marshal(snapshot)
	if err != nil {
		return errors.Wrap(err, "encode execution")
	}
	var result sql.Result
	if expectedRevision == 0 {
		result, err = s.db.ExecContext(ctx, `INSERT INTO converge_execution(config_id, revision, payload)
VALUES(?, ?, ?) ON CONFLICT(config_id) DO NOTHING`, id.Name, snapshot.Revision, payload)
	} else {
		result, err = s.db.ExecContext(ctx, `UPDATE converge_execution SET revision = ?, payload = ?
WHERE config_id = ? AND revision = ?`, snapshot.Revision, payload, id.Name, expectedRevision)
	}
	if err != nil {
		return errors.Wrap(err, "commit execution")
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "read execution CAS result")
	}
	if rows != 1 {
		return ErrGenerationChanged
	}
	return nil
}

func (s *SQLiteStore) DeleteExecution(ctx context.Context, id model.ConfigID) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM converge_execution WHERE config_id = ?`, id.Name)
	return errors.Wrap(err, "delete execution")
}

func (s *SQLiteStore) Append(ctx context.Context, event model.Event) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return errors.Wrap(err, "encode journal event")
	}
	var key any
	if event.EventID != "" {
		key = event.EventID
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO converge_journal(config_id, event_key, payload)
VALUES(?, ?, ?) ON CONFLICT(event_key) DO NOTHING`, event.ConfigID, key, payload)
	return errors.Wrap(err, "append journal event")
}

func (s *SQLiteStore) Events(ctx context.Context, configID string) ([]model.Event, error) {
	query := `SELECT payload FROM converge_journal`
	var args []any
	if configID != "" {
		query += ` WHERE config_id = ?`
		args = append(args, configID)
	}
	query += ` ORDER BY sequence`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, errors.Wrap(err, "list journal events")
	}
	defer rows.Close()
	var events []model.Event
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return nil, errors.Wrap(err, "scan journal event")
		}
		var event model.Event
		if err := json.Unmarshal(payload, &event); err != nil {
			return nil, errors.Wrap(err, "decode journal event")
		}
		events = append(events, event)
	}
	return events, errors.Wrap(rows.Err(), "iterate journal events")
}

var _ StateStore = (*SQLiteStore)(nil)
var _ ExecutionStore = (*SQLiteStore)(nil)
var _ Journal = (*SQLiteStore)(nil)
var _ DesiredSnapshotStore = (*SQLiteStore)(nil)
