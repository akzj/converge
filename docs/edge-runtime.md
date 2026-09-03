# Edge Runtime integration contract

The `edge` package is the production boundary between a Veyra server and
Converge Core. The server sends complete desired snapshots; Core remains an
embedded reconciliation engine and does not own transport or authentication.

## Acceptance and execution

1. The server sends `model.DesiredSnapshot` with a monotonically increasing
   global revision and a canonical snapshot digest.
2. Every item must have a non-zero version, valid spec digest, unique ConfigID,
   and an acyclic dependency graph whose dependencies are present in the same
   snapshot.
3. The runtime persists the entire snapshot and all per-Config version
   high-water marks in one SQLite transaction.
4. Only after commit does the API return an accepted ACK. Retrying the same
   revision and digest is idempotent; conflicting or older revisions are
   rejected.
5. The runtime asynchronously dispatches each Desired to Core in dependency
   order. Configurations omitted by the snapshot receive a conditional delete;
   that delete cannot remove a newer Desired that arrived meanwhile.
6. Startup replays the latest accepted snapshot after Core recovery. Thus a
   crash after ACK but before dispatch or deletion does not lose intent.

`dispatched_revision` means the snapshot has been handed to Core. It does not
mean every configuration has converged. Per-configuration status is the source
for convergence.

## HTTP surface

`edge.NewHTTPHandler` provides:

- `POST /v1/desired-snapshots` — durable accepted/rejected ACK;
- `GET /v1/desired-snapshots/current` — reconnect/version negotiation;
- `GET /v1/status` and `GET /v1/status/{config}` — convergence, observed facts,
  nodes, attempts, effects, controls, and errors without Provider payloads;
- `POST /v1/commands/refresh/{config}` — explicit inspection/replan command;
- `GET /healthz` — process liveness;
- `GET /readyz` — Core recovery completed and runtime running;
- `GET /metrics` — bounded-cardinality Prometheus text metrics.

All endpoints except liveness require a bearer token. Production embedding must
terminate TLS and rotate/provision that token; the package deliberately does
not invent a Veyra identity system.

## SQLite operation

`OpenSQLite` takes a non-blocking advisory lock on `<database>.lock`. One Edge
Agent process must exclusively own a database and its managed resources.
SQLite uses WAL, FULL synchronous durability, foreign keys, and a bounded busy
timeout. Schema migrations use `PRAGMA user_version` and run transactionally.

Use `SQLiteStore.Backup` to create a consistent online backup. Restore is an
offline operation: stop the owner, preserve the failed database and WAL files,
place the backup at the configured path, and start the agent. Opening a schema
newer than the binary supports fails closed.

## Failure semantics

- No durable write means no accepted ACK.
- A lost ACK is handled by replaying the same revision and digest.
- Provider or planning failure is reported per Config and does not invalidate
  the accepted Snapshot.
- Running attempts found after process death become `Unknown`; recovery must
  inspect external state before retrying side effects.
- Network availability and telemetry export never gate local reconciliation.

Provider `Inspect`, `Replan`, `Verify`, `Execute`, and effect-control calls run
behind bounded worker pools and deadlines. Providers must honor context
cancellation; Go cannot forcibly stop a Provider that ignores its context.

The repository tests include database reopen recovery and a real subprocess
kill while an Attempt is durably Running. They do not constitute a long-running
soak or filesystem-corruption campaign.
