# ADR 0001: Generation-aware replanning and supersession

- Status: Accepted
- Date: 2026-07-28

## Context

Converge currently cancels every running operation when a new desired revision
arrives. This loses valid work when plans overlap, cannot distinguish late
old-plan events, and cannot safely reason about non-cancellable effects.
A desired revision, an execution plan, and an operation attempt are distinct.

## Decision

### Identity

Each plan has a unique `PlanID` and a per-config monotonic `Generation`. Each
operation has a stable logical `Key`, a semantic `Fingerprint`, and each
execution has a unique `AttemptID`. Events carry all four identities and may
mutate only the matching attempt.

The fingerprint covers provider digest, action, input, phase, destructive flag,
cancellation mode, conditions, timeout, handler, and dependency keys. A reused
ID alone never establishes semantic equality.

### Provider planning contract

Planning receives observed state, desired state, an active-plan snapshot, and
active effects. This prevents planning as though in-flight work did not exist.
A legacy `Diff` adapter is retained during migration, with conservative reuse.

### Replanning protocol

1. Snapshot active generation, nodes, attempts, and newest desired revision.
2. Inspect and build a provisional candidate outside the reconciler lock.
3. Validate and classify nodes as carry-forward, drop, cancel, drain, or add.
4. Atomically install only if generation and desired revision remain current;
   otherwise discard the candidate and replan.
5. Route events by plan/generation/key/attempt identity. Retired events are
   journalled but cannot advance the active graph.

Candidate failure never damages the active plan.

### Carry-forward

A node transfers only when key, fingerprint, provider digest, and dependency
closure are compatible and its lifecycle state permits transfer. Pending,
ready, running, and completed state may transfer. A running transfer retains the
same `AttemptID` and is not started again.

### Cancellation lifecycle

Requesting cancellation moves an attempt to `Cancelling`; it does not assert
that cancellation completed. Old-only pending/ready nodes are dropped.
Old-only safe/async running nodes cancel; non-cancellable nodes drain. Both stay
tracked as retired attempts until a terminal event. Conflicting new work is
blocked by a barrier while retired effects remain active.

## State machine

```text
Pending -> Ready -> Running -> Completed
                     |  |  \
                     |  |   -> Failed
                     |  -> Cancelling -> Cancelled | Completed | Failed
                     -> Draining -----> Completed | Failed

Failed --retry/new AttemptID--> Pending
```

## Invariants

1. An `AttemptID` executes at most once.
2. An event updates only its named attempt.
3. A retired event never advances the active generation unless explicitly
   carried forward.
4. An invalid candidate never damages the active plan.
5. `Cancelling` is not `Cancelled` before provider acknowledgement.
6. Retired cancel/drain completion triggers inspect and replan.
7. Conflicting old and new effects never run concurrently.
8. Desired versions advance monotonically; equal version with unequal digest is
   a conflict.
9. Plan installation is compare-and-swap on generation and desired revision.
10. Completed equivalent nodes do not execute again.

## Testing

Tests are state-machine and invariant driven: table tests for fingerprint and
compatibility; every valid/invalid transition; plan classification; stale,
duplicate, and reordered events; rapid v1/v2/v3 replans; cancellation/drain
barriers; provider upgrades; dependency changes; randomized invariant tests;
and the complete suite under the race detector.

## Consequences

The model and Provider contract grow, and active plans are separated from
retired attempts. Supersession becomes deterministic, auditable, and safe under
concurrency. Legacy providers migrate through an adapter and receive
conservative reuse until they provide stable semantic identity.
