# ADR 0001: Generation-aware planning, durable execution, and supersession

- Status: Accepted
- Date: 2026-07-28
- Last updated: 2026-07-29

## Context

A desired revision, execution plan, operation attempt, and long-lived external
effect are different entities. Treating a new desired revision as "cancel every
operation and overwrite the graph" loses reusable work, permits late events to
corrupt current state, and cannot safely recover non-cancellable effects.

The engine must remain correct across concurrent desired updates, provider
upgrades, duplicate/reordered events, persistence failures, and process crashes.

## Decision

### Identity

Each plan has a unique `PlanID` and monotonic per-config `Generation`. Each
operation has a provider-defined stable `OperationKey` and a Core-verified
semantic `Fingerprint`. Every execution has a cryptographically random,
non-reused `AttemptID`.

Events identify the complete tuple:

```text
PlanID + Generation + OperationKey + AttemptID
```

A carried running attempt preserves its source plan/generation and records
`CarriedTo`; its already-running worker must not emit a fabricated new identity.

The fingerprint covers provider type and digest, action, canonical input,
phase, destructive flag, cancellation mode, conditions, timeout, handler,
conflict key, and dependency keys. Identity fields and runtime state are not
fingerprinted.

### Plan generation versus execution revision

`Generation` changes only when a new plan is installed. `ExecutionSnapshot`
also has an independent monotonic `Revision`, incremented for every attempt,
waiting, retry, resolution, deletion, and outbox transition.

Execution persistence uses compare-and-swap on `Revision`; generation alone
cannot prevent lost same-plan updates in a multi-writer store.

### Provider planning and version binding

`Provider.Replan` receives observed state, the complete desired state, an active
plan snapshot, active/retired attempts, and provider digest. Core normalizes and
validates candidate operations before installation.

Provider implementations are registered by `(type, digest)`. An attempt always
executes against the implementation whose digest is embedded in its plan;
replacing the current provider never causes an old plan to run with new opaque
semantics.

There is no legacy `Diff` compatibility path.

### Replanning protocol

1. Snapshot active generation, execution revision, attempts, and desired state.
2. Inspect and provisionally replan outside the reconciler lock.
3. Resolve any Unknown external effects from authoritative provider inspection.
4. Validate operation identity, fingerprints, dependencies, and DAG acyclicity.
5. Classify old/new nodes as carry, drop, cancel, drain, or add.
6. Install by generation/revision CAS; stale candidates are discarded.
7. Route lifecycle events by complete attempt identity.

An invalid candidate or failed persistence write never modifies published
in-memory state.

### Carry-forward

A node transfers only when key, fingerprint, provider digest, dependency closure,
and lifecycle state are compatible. Pending, Ready, Running, and Completed may
carry. A Running carry retains its AttemptID and worker.

Waiting currently uses the conservative `Drop + Add` policy. Its external JobID
or provider checkpoint may be reused, but the old polling attempt is retired and
the next poll gets a fresh AttemptID.

### Attempt lifecycle

```text
Pending -> Ready -> Running -> Completed
                     |  |  |
                     |  |  +-> Waiting -> Pending (fresh AttemptID)
                     |  +----> Failed -> Pending (retry, fresh AttemptID)
                     +-------> Cancelling -> Cancelled | Completed | Failed
                     +-------> Draining ----> Completed | Failed
                     +-------> Unknown ------> provider resolution
```

Normal Waiting does not consume retry budget. Retryable failure retires the old
attempt and is bounded. Waiting wakeup and retry always allocate a new AttemptID.

### Timeout and Unknown effects

Context cancellation or deadline only proves that Core stopped waiting; it does
not prove an external mutation stopped. A timed-out attempt becomes Unknown and
retired, and its conflict barrier remains active.

During Inspect/Replan the provider resolves Unknown effects as:

```text
StillActive | Completed | Absent
```

Completed advances the node, Absent safely requeues it, and StillActive keeps
the barrier. Resolutions are durable CAS transitions.

### External long-lived jobs

An Attempt is a short control/inspection call, not a 100GB download. A durable
external download service must expose a stable JobID/EffectID and idempotent
CreateOrGet, Inspect, cancellation, and recovery semantics.

```text
Core Attempt       = one short RPC or poll
Core Active Effect = durable reference to an external job
External Job       = actual long-lived transfer state
```

Attempt context cancellation never substitutes for cancelling an external job.
Supersession/deletion must request durable effect cancellation and await provider
resolution. Polling/Waiting is the correctness mechanism; callbacks are only a
latency optimization.

For a local time-sliced downloader, no background job survives Execute: each
attempt downloads a bounded Range slice, durably checkpoints the partial file,
and returns Waiting.

### Conflict barriers

Operations default to a config-scoped `ConflictKey`; providers may use a more
specific resource key. Cancelling, Draining, and Unknown retired effects block
new operations with the same conflict key until a terminal resolution.

### Durable outbox and Journal

Provider results are first persisted in the execution snapshot outbox, then
published by a single dispatcher. Failed publication leaves the event pending.
State transition failure does not acknowledge the event.

After a successful or explicitly duplicate transition, the outbox event is
acknowledged. Journal append is idempotent by `EventID`, because outbox delivery
is at least once.

### Crash recovery

Plans durably contain the complete `DesiredState`, including Spec and config
dependencies. Recovery uses the union of final recorded state and execution
snapshots; an execution-only first convergence is not orphaned.

Running/Cancelling/Draining attempts recover conservatively as Unknown and must
be resolved by provider inspection. Provider registration resumes affected
plans after recovery.

### Deletion

Deletion is a durable tombstone lifecycle, not two immediate map deletions:

```text
MarkDeleting
 -> stop new scheduling
 -> delete dependents according to dependency policy
 -> cancel/drain/resolve active effects
 -> delete recorded state
 -> delete execution tombstone
 -> remove in-memory config
```

A crash preserves the tombstone and recovery continues deletion. External jobs
must be cancelled through provider effect protocol rather than attempt context.

### Configuration dependencies

Config dependency cycles are rejected. An upstream change transitively
invalidates all downstream configs. Current deletion policy cascades through
transitive dependents before deleting the upstream.

## Invariants

1. An AttemptID is globally non-reused and starts at most once.
2. An event updates only its complete identity tuple.
3. Retired events never advance an active generation unless explicitly carried.
4. Invalid candidates and failed persistence never damage published state.
5. Cancelling is not Cancelled before acknowledgement.
6. Unknown effects retain conflict barriers until provider resolution.
7. Conflicting old and new effects never execute concurrently.
8. Desired versions advance monotonically; equal version with unequal digest is
   rejected.
9. Plan install uses generation CAS; every execution transition uses revision
   CAS.
10. Completed equivalent dependency closures do not execute again.
11. Provider execution digest equals the plan provider digest.
12. Outbox events are not acknowledged before their state transition succeeds.
13. Deleting configs schedule no new operations.
14. Complete Desired state is recoverable for every active plan.

## Testing

The suite includes table/state-machine tests, deterministic randomized DAG
classification, duplicate/stale identity events, persistence fault injection,
outbox crash recovery, waiting/retry end-to-end flows, provider upgrade binding,
timeout/Unknown barriers, dependency cycles/cascades, deletion tombstones, and
race-detector runs.

Production backends must additionally test real transactional isolation,
multi-process revision CAS, process kill/restart windows, provider external-job
recovery, and network partitions.

## Consequences and remaining limits

The architecture favors correctness and explicit durable state over a small API.
Providers must expose stable semantic operation identity and authoritative effect
resolution.

Current repository limitations:

- only in-memory StateStore, ExecutionStore, EventBus, Arbiter, and Journal are
  implemented;
- Registry persistence still occurs while holding a global lock; a production
  implementation should use per-config serialization plus revision CAS;
- attempt history needs a compaction/retention policy;
- fully equivalent periodic drift can still install a new generation;
- a complete first-class `ActiveEffect` record with ExternalJobID and durable
  RequestCancel protocol remains required for independent download daemons;
- production deletion should place tombstone, recorded state, and execution
  state in one transactional store or a formally recoverable saga.
