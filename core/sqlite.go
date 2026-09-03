package core

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	_ "modernc.org/sqlite"

	"github.com/akzj/converge/pkg/model"
)

// SQLiteStore is a durable implementation of StateStore, ExecutionStore, and
// Journal. Execution updates use a revision-guarded statement, so competing
// writers cannot silently overwrite a newer snapshot.
type SQLiteStore struct {
	db *sql.DB
}

// OpenSQLite opens or creates a SQLite database suitable for an edge runtime.
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
	dsn := (&url.URL{Scheme: "file", Path: abs, RawQuery: "_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)"}).String()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, errors.Wrap(err, "open sqlite")
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)
	store := &SQLiteStore{db: db}
	if err := store.init(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *SQLiteStore) init(ctx context.Context) error {
	const schema = `
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
	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return errors.Wrap(err, "initialize sqlite schema")
	}
	return nil
}

func (s *SQLiteStore) Close() error { return s.db.Close() }

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
