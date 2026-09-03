# Runtime integration contract

The root `converge` package is the library boundary used by an embedding Agent.
The Agent supplies complete desired snapshots; Converge does not own transport,
authentication, or the server protocol.

## Acceptance and execution

1. The embedding Agent submits `model.DesiredSnapshot` with a monotonically
   increasing global revision and a canonical snapshot digest.
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

## SQLite operation

`OpenSQLite` takes a non-blocking advisory lock on `<database>.lock`. One
embedding Agent process must exclusively own a database and its managed resources.
SQLite uses WAL, FULL synchronous durability, foreign keys, and a bounded busy
timeout. Schema migrations use `PRAGMA user_version` and run transactionally.

Use `Runtime.Backup` to create a consistent online backup. Restore is an
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

For embedded lifecycle ownership, cancel the context passed to `Runtime.Run`,
wait for it to return, then call `Runtime.Close` and close Provider resources
and the asynchronous Observer. `Run` waits for tracked Core workers and is single-use.
`OpenSQLiteRuntime` accepts `core.ReconcilerOption` values so the
embedding Agent can install its logger and Observer without bypassing the
convenience constructor.

Register Providers through `Runtime.RegisterProvider`; the root package keeps
the Reconciler and SQLite store private. Provider implementations remain
digest-bound to durable Plans. After the runtime has stopped, an embedding
Agent may call `Runtime.UnregisterProviderVersion`; Converge rejects removal of
the current version or any version still referenced by durable Plan, Effect, or
Control state.

The repository tests include database reopen recovery and a real subprocess
kill while an Attempt is durably Running. They do not constitute a long-running
soak or filesystem-corruption campaign.
