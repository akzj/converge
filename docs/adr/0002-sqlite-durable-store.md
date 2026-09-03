# ADR 0002: SQLite durable store

## Status

Accepted for the embedded single-process runtime.

## Decision

`core.SQLiteStore` implements `StateStore`, `ExecutionStore`, and `Journal` in
one SQLite database. The database uses WAL mode and a busy timeout.

Execution snapshots are stored as versioned JSON blobs behind a SQL revision
compare-and-swap. A caller may publish an in-memory transition only after that
CAS succeeds. Desired state is included in the execution snapshot before
planning, so a successful `SubmitDesired` survives restart even when no plan
could be produced.

Journal rows are ordered by an SQLite sequence. Non-empty event IDs are unique,
making replayed appends idempotent.

## Scope and limitations

- The schema is intentionally private to Converge; migrations must preserve
  decoding of prior execution snapshots.
- `EventBus` remains an in-process notification mechanism. Durable delivery is
  provided by the execution outbox, not by the bus itself.
- `Arbiter` remains process-local and is suitable only when one Edge Agent owns
  the database and managed resources.
- `PlanRegistry` serializes transitions per Config. It releases the registry
  map lock while an `ExecutionStore` write is in progress, so slow persistence
  for one Config does not block unrelated Configs. SQLite still has one writer
  at a time internally; `busy_timeout` bounds that database-level contention.
