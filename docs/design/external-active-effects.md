# External Active Effects: Technical Design

- Status: Accepted for Phase A implementation
- Owner: Converge Core
- Last updated: 2026-07-29
- Related: [ADR 0001](../adr/0001-generation-aware-replanning.md)
- Normative API/model: [`external_effects_contract.go.txt`](external_effects_contract.go.txt)

## 1. Purpose and authority

This design governs work that outlives one Core attempt, such as a 100 GB file
transfer performed by an independent download service. It is an implementation
alignment contract: divergent code requires this document to be updated and
reviewed first.

The compile-checked contract file linked above is the **normative source** for
names, fields, enums, DTOs, Provider methods, and RegistryCommands. This document
is normative for lifecycle, ordering, invariants, and acceptance gates. If a
snippet here conflicts with the contract file, the contract file wins and this
document must be corrected before implementation continues.

## 2. Scope

In scope:

- durable external effect and plan ownership reference;
- stable identity across polling, replanning, restart, and provider upgrade;
- idempotent ensure, observe, reference release, and recovery;
- Waiting, supersession, deletion, shared jobs, crash consistency, and outbox;
- a fake download service vertical slice.

Not in the first vertical slice:

- real byte transfer, production transport/authentication, bandwidth scheduling;
- production SQL storage or multi-process outbox leasing;
- push notifications as a correctness dependency;
- direct Core-initiated cancellation of a referenced shared job.

## 3. Closed Phase 0 decisions

1. Lifecycle is two nodes: `effect_ensure` completes after durable job binding;
   downstream `effect_observe` polls through Waiting until terminal.
2. `ActiveEffect` is the stable external job binding; `EffectReference` is plan
   ownership. Plan carry transfers references and never rewrites job identity.
3. The external service is authoritative for references and zero-reference
   cancellation. Core never computes global refcount.
4. `model.Operation` receives mandatory typed `ExecutionKind` and `EffectKey`.
   Core never interprets provider-specific Action.
5. Immutable IdempotencyKey, ArtifactID, SemanticFingerprint, EnsureSpec, and
   ReferenceID are persisted before Ensure RPC.
6. External revision is mandatory and monotonic.
7. Direct Attempt resolution and external Effect observation are separate.
8. Effect/reference/node/attempt/outbox changes occur in one execution Revision
   CAS through typed RegistryCommands.
9. Ordinary 404/transport failure is Unknown/Retry, never Absent. Only an
   authoritative matching Gone response is Absent.
10. Deletion fails closed until resolution or an audited administrator override.
11. The service's last-reference release disposition initiates cancellation;
    `RequestCancelEffect` is not part of the first contract.

## 4. Identity and lookup

The following identities are distinct:

| Identity | Meaning |
|---|---|
| PlanID + Generation | immutable desired plan |
| OperationKey | one plan node |
| AttemptID | one short Provider call/poll |
| EffectKey | config-scoped logical effect slot |
| EffectID | globally unique Core external-effect identity |
| ReferenceID | one plan's ownership of a logical effect slot |
| ExternalJobID | download service job |
| ArtifactID | immutable content digest |
| PollRequestID | one poll request/reply identity |

Normative composite identities:

```text
LogicalEffectSlot = (ConfigID, EffectKey)
ReferenceID       = (ConfigID, PlanID, Generation, EffectKey)
```

Ensure, observe, and release nodes in one plan declare the same EffectKey and
share one ReferenceID. OperationKey routes the node and is not an ownership
identity. Lookup selects the exact active-plan ReferenceID, dereferences EffectID,
and verifies ArtifactID and SemanticFingerprint. The same EffectKey with changed
artifact/fingerprint creates a new Effect; it never mutates the old binding.
Multiple generations can temporarily have different References to one Effect.

## 5. Architecture

```text
Converge Core
  desired / plan / attempt / effect / reference / control / outbox
                 |
                 | short typed Provider calls
                 v
Download Provider
  protocol adapter and state translation
                 |
                 | RPC/HTTP
                 v
Download Service
  durable Job DB, references, scheduler, workers, partial data, verification
```

An Attempt is one short control RPC. An ActiveEffect is a durable Core reference
to a long-lived External Job. Attempt context cancellation never means that an
external job was cancelled.

## 6. Durable execution model

The exact structures are in the normative contract. `ExecutionSnapshot` will
persist:

```text
Effects []ActiveEffect
EffectReferences []EffectReference
EffectControls []EffectControl
```

They share the same monotonic execution Revision CAS as plan, attempts, deleting
tombstone, and outbox.

### 6.1 Binding validation

| Binding | Allowed states | Job binding |
|---|---|---|
| Unbound | Ensuring, Unknown, CancelRequested, Failed(authoritative pre-job rejection only) | JobID empty, external revision 0 |
| Bound | Active, CancelRequested, Cancelling, Completed, Cancelled, Failed, Unknown | JobID non-empty, revision > 0 |

A late exact Ensure success changes Unbound to Bound without clearing a
CancelRequested intent. No later transition may replace a bound JobID.

### 6.2 Durable control scheduler

EffectControl is the recoverable scheduler record. It stores kind, request ID,
EffectID, ReferenceID, NextCheckAt, RetryCount, state, in-flight AttemptID,
PollRequestID, and LeaseExpiresAt. Kinds are EnsureRetry, Observe, Release, and
ObserveCancellation.

Recovery derives no ambiguous action from EffectState alone; it resumes persisted
EffectControls. Each control stores ConfigID and Provider type/digest for routing.
The scheduler first calls `ListDueControls(now)` to obtain stale-able
`(ConfigID, ControlRequestID)` references, then calls `ClaimDueControl` with that
exact pair. Claim atomically validates the current config Execution Revision,
control due/state, and provider binding, and binds fresh AttemptID, PollRequestID,
and lease. A stale list entry returns Stale/NotDue and is harmless.

Control state transitions are:

```text
Pending/Yielded -> InFlight (fresh AttemptID + PollRequestID + lease)
InFlight -> Yielded (clear identities, set NextCheckAt)
InFlight -> Completed
InFlight with expired lease -> Pending (clear identities, increment retry)
```

A response must match the currently bound AttemptID and PollRequestID. Replies
from a reclaimed lease are stale and cannot mutate the new claim. Reclaim itself
is a Revision CAS. Required recovery tests cover crash while InFlight, late old
reply after reclaim, and fresh claim completion.

### 6.3 Typed operation routing

The fields live directly on `model.Operation` and are included in its fingerprint:

```text
ExecutionKind: direct | effect_ensure | effect_observe | effect_release
EffectKey
```

Validation rules:

- direct requires empty EffectKey;
- effect kinds require non-empty EffectKey;
- each observe has one compatible upstream ensure with the same EffectKey;
- release uses the same EffectKey or an explicitly retired Reference;
- ensure artifact/fingerprint is unique per EffectKey within a plan.

## 7. Provider control contract

Reference activation rules are normative: a newly created plan Reference starts
`ReferenceEnsuring`. It becomes `ReferenceActive` only after the service confirms
that exact ReferenceID is durable through Ensure/EnsureReference. Observe nodes
require Active. The prior generation Reference cannot enter ReleaseRequested
until the replacement Reference is Active. Transient/Unknown ensure-reference
outcomes retain Ensuring and retry the same request; authoritative permanent
failure fails the new plan closed without releasing the old Reference.

Ensure and EnsureReference use the typed `EnsureFailureKind` in the normative
contract:

- None: apply the disposition;
- TransientKnownNotApplied: yield/backoff, remain Ensuring;
- UnknownOutcome: Effect becomes Unknown Unbound (initial ensure) or Reference
  remains Ensuring (add-reference), retry same immutable request;
- AuthoritativeRejected: service guarantees job/reference was not created;
  initial Effect fails with ResolutionRequired=false, while add-reference fails
  the new plan and preserves the old Active Reference.

The normative, compile-checked DTOs and interfaces are in
`external_effects_contract.go.txt`.

Key rules:

- Ensure request is built only from durable ActiveEffect + EffectReference;
  Provider must not rebuild it from a newer mutable plan.
- Observe request and result carry exact AttemptID and PollRequestID.
- Observe returns a map keyed PollRequestID with exactly one result per request;
  this supports multiple generation References polling the same Effect.
- Release operates on exact ReferenceID and deterministic ReleaseRequestID.
- The service returns `StillReferenced`, `LastReferenceCancelRequested`, or
  `Released`; Core does not infer reference count.
- Responses echo all identities and external revision.

External revision handling:

```text
lower revision                    -> stale, ignore
same revision + same disposition  -> effect duplicate
same revision + different result  -> protocol violation
higher revision                   -> apply only if legal transition
```

Effect duplicate does not imply Attempt duplicate. A fresh polling Attempt still
must yield/complete using its exact PollRequestID.

## 8. Effect state and barrier semantics

### 8.1 Transition rules

| From | Input | To | ResolutionRequired after | Node/control result |
|---|---|---|---|---|
| Ensuring Unbound | bound | Active Bound | true | ensure node Completed; schedule Observe |
| Ensuring Unbound | authoritative rejection before job creation | Failed | false | node Failed |
| Ensuring Unbound | RPC outcome unknown | Unknown Unbound | true | schedule EnsureRetry |
| Ensuring Unbound | delete/supersede | CancelRequested Unbound | true | retain immutable ensure identity |
| CancelRequested Unbound | late matching bound | CancelRequested Bound | true | schedule Release |
| Active Bound | still active | Active | true | AttemptYielded; schedule Observe |
| Active Bound | completed | Completed | false | observe node Completed |
| Active Bound | transport unknown | Unknown Bound | true | schedule Observe |
| CancelRequested Bound | last-ref cancel requested | Cancelling | true | schedule ObserveCancellation |
| Cancelling | still active | Cancelling | true | AttemptYielded |
| Cancelling | completed race | Completed | false | continue release/deletion |
| Cancelling | cancelled | Cancelled | false | continue release/deletion |
| Cancelling | transport unknown | Unknown Bound | true | schedule ObserveCancellation |
| Unknown Unbound | ensure retry bound | Active or CancelRequested Bound | true | based on durable intent |
| Unknown Bound | observed active | Active | true | schedule Observe |
| Unknown Bound | completed/cancelled | Completed/Cancelled | false | terminal |
| Unknown Bound | authoritative Gone | remove; recreate if desired | false then true | schedule Ensure if desired |
| any cancel/release unresolved permanent failure | Failed | Failed | true | fail closed/admin action |
| Completed/Cancelled/Failed | release confirmed and journal gate passed | removed/compacted | false | none |

`ResolutionRequired` is persisted. It is false only for provider-confirmed
terminal state, authoritative pre-job rejection, authoritative Gone, or completed
release/compaction.

### 8.2 Barrier function

```text
BlocksConflict(effect) = state in {
    Ensuring, Active, CancelRequested, Cancelling, Unknown
} OR effect.ResolutionRequired

Blocked(operation, effect) = BlocksConflict(effect)
                             AND same ConflictKey
                             AND NOT IsControlForExactEffect(operation, effect)
```

Exact effect_ensure retry, effect_observe, effect_release, and cancellation
observation may cross their own EffectID+ReferenceID barrier. They remain blocked
by other conflicting effects. Direct, activation, and new-effect operations
never receive an exemption.

## 9. Protocols

### 9.1 Ensure and poll

1. Install validated ensure -> observe plan.
2. `BeginEnsureEffect` persists Unbound Ensuring Effect, immutable spec,
   Reference, and control in one Revision CAS.
3. Provider calls the external atomic idempotent operation
   `CreateOrGetJobAndEnsureReference(IdempotencyKey, ReferenceID, EnsureSpec)`.
   The service returns Bound only after both Job and Reference are durable.
4. `ApplyEnsureResult` atomically writes JobID/revision, Active state, ensure-node
   Completed, Observe control, and lifecycle outbox.
5. Observe control claims a fresh AttemptID/PollRequestID.
6. Active result atomically yields the Attempt, writes NextCheckAt, and keeps the
   Effect barrier.
7. Completed result atomically completes Attempt/node/effect and enqueues outbox.

If Job creation succeeds but reference insertion or response/persistence is lost,
retrying the same atomic external operation with the same durable
IdempotencyKey/EnsureSpec/ReferenceID repairs the reference and returns the same
Job. The service must not report Bound or make a zero-reference Job eligible for
GC before that Reference is durable.

### 9.2 Late Ensure and Unknown without JobID

Delete/supersession can set CancelRequested while Ensure RPC is in flight. A late
success is accepted only for the exact durable request identity. Core persists
JobID/revision while retaining CancelRequested, then schedules Release.

Unknown Unbound cannot be observed. It schedules EnsureRetry with the original
immutable request. Unknown Bound schedules Observe. These paths survive restart.

### 9.3 Supersession

- Same artifact/fingerprint: add/transfer the new plan Reference; reuse Effect/Job.
- Changed artifact: create B; old Reference becomes ReleaseRequested.
- Multiple generation References may coexist during transition.
### 9.3.1 Same-effect reference transfer

When a new generation needs the same Effect semantic fingerprint, Core creates a
new deterministic ReferenceID and schedules `ControlEnsureReference`. Provider
`EnsureReference` idempotently adds that ReferenceID to the already-bound
ExternalJobID; `ApplyEnsureReferenceResult` must succeed before the new plan's
observe node may run or the old Reference may be released. The service echoes
EffectID, new ReferenceID, request ID, JobID, and revision.

A crash or lost response retries the same EnsureReference request. This command
never creates or replaces an Effect/Job binding. Only after the new reference is
durable may supersession schedule release of the prior generation reference.

- Never select or cancel by EffectKey alone.

### 9.4 Shared release, cancellation, and deletion

```text
MarkDeleting / supersede old reference
 -> persist ReferenceReleaseRequested + Release control
 -> RemoveReference(ReferenceID, ReleaseRequestID)
 -> service response:
      StillReferenced:
        local Reference Released; never cancel Job
      LastReferenceCancelRequested:
        retain Effect binding; Effect Cancelling; observe terminal
      Released:
        Reference Released; job already terminal/removed
 -> journal/outbox gate
 -> compact reference/effect when safe
 -> continue/finalize deletion
```

Core never direct-cancels a referenced job. A future direct-cancel API requires a
service-issued exclusive capability and is outside this slice. Permanent failure
is fail-closed: tombstone and barrier remain until authorized audited override.

### 9.5 Crash recovery

Recover full Desired, plan, attempts, Effects, References, Controls, outbox,
tombstone, and Revision. Resume due controls; do not infer job absence from Core
restart. Provider registration restores the digest-specific adapter. Polling is
the correctness path; callbacks only wake controls early.

## 10. Atomic registry commands and idempotency

The compile-checked `RegistryCommands` interface is normative. Every applied
command performs one execution Revision CAS containing all related effect,
reference, control, node, attempt, and outbox mutations.

```text
EffectMutationID = effect/<EffectID>/<ExternalRevision>/<disposition>
AttemptEventID   = attempt/<AttemptID>/<PollRequestID>/<yield|complete|fail>
```

A repeated external revision/disposition can be duplicate for the Effect while a
new polling Attempt still transitions with its unique AttemptEventID. Stale
commands do not mutate/outbox. Rejected protocol input is dead-lettered. Storage
failure retains the control/event for retry.

The Provider Ensure call represents the service's atomic
`CreateOrGetJobAndEnsureReference` operation. `EnsureBound` is illegal unless
both the job and exact ReferenceID are durable in the service.

Ordering:

```text
Ensure:
  persist immutable Effect/Reference/Control
  -> external CreateOrGet
  -> atomic result/node/control/outbox CAS

Observe:
  claim control + AttemptID/PollRequestID
  -> external Get
  -> atomic effect/attempt/node/control/outbox CAS

Release:
  persist ReleaseRequested + control
  -> external RemoveReference
  -> atomic reference/effect/control/outbox CAS
ID generation responsibility is fixed: the Core caller cryptographically
generates EffectID and deterministically derives ReferenceID/ControlRequestID
before calling Begin. Begin only validates and idempotently persists supplied
identity/spec; identical repeats return Duplicate, while the same identity with
different immutable input is Rejected. Begin never generates or returns an ID.

Release failure taxonomy is normative:

- Transient: keep control, backoff/yield, no terminal transition;
- UnknownOutcome: keep ResolutionRequired and retry the same RequestID;
- Permanent: Effect Failed with ResolutionRequired=true, fail closed;
- None: apply typed release disposition and external revision.

ExecutionSnapshot validation requires unique EffectID, ReferenceID, and
ControlRequestID; every Reference and Control resolves to an existing Effect;
every in-flight Control has non-empty unique AttemptID and PollRequestID.

```

## 11. Fake download service vertical slice

```go
type FakeDownloadService struct {
    Jobs       map[string]Job
    ByIdemKey  map[string]string
    References map[string]map[string]JobReference // JobID -> ReferenceID -> ref
}
```

It supports atomic deterministic `CreateOrGetJobAndEnsureReference`, monotonic
revisions, idempotent RemoveReference, explicit test-driven job state
advancement, last-reference
cancellation disposition, and injectable timeout/Gone/stale-revision errors. It
is a correctness simulator, not a production backend.

## 12. Implementation phases and gates

### Phase 0: design closure

Gate:

- normative contract compiles;
- no blocker in independent review;
- transition/barrier/control/reference protocols are unambiguous.

### Phase A: model only

Deliver Effect/Reference/Control persistence, deep clone, validators, Operation
routing fields/fingerprint. No runtime behavior change.

Gate: table-test every legal/illegal transition, binding rule, barrier exemption,
lookup rule, and snapshot recovery.

### Phase B: Provider contract and fake service

Gate: Ensure idempotency, strict response identity, revision rules, reference
idempotency, last-reference cancellation, batch cardinality.

### Phase C: ensure/observe vertical path

Gate: queued -> downloading -> ready; fresh Attempt/Poll IDs; stable Effect/Job;
restart in Unbound/Bound/Waiting resumes correctly; duplicate effect revision
still yields each fresh poll Attempt.

### Phase D: supersession and deletion

Gate: same artifact reuses Job; changed artifact releases old Reference; shared
Job is not cancelled; last reference cancellation and completion race; restart
in ReleaseRequested/Cancelling/Deleting.

### Phase E: faults and randomized tests

Inject failure at each RPC/CAS/outbox boundary and randomize response ordering.
Gate: build, test, race, vet; effect state-machine coverage >= 85%.

No phase starts until the prior gate is committed and independently reviewed.

## 13. Invariants

1. AttemptID, PollRequestID, EffectID, ReferenceID, and ExternalJobID are distinct.
2. CreateOrGet and reference operations are idempotent across crash windows.
3. One Effect has at most one bound ExternalJobID; it is never replaced.
4. One plan logical slot has one ReferenceID shared by ensure/observe/release.
5. Transport failure never proves Absent or Cancelled.
6. Effect control operations cross only their own exact barrier.
7. Non-control conflicting work never crosses a blocking effect.
8. Shared Jobs are never cancelled while the service reports another reference.
9. Effect mutation and polling Attempt lifecycle have separate idempotency IDs.
10. Every transition/outbox write is one execution Revision CAS.
11. Failed transition is never acknowledged.
12. Stale plan/attempt/poll/effect/external revisions never mutate current state.
13. Deleting configs start no new operations/effects and fail closed.
14. Complete Desired and every nonterminal Effect/Reference/Control recover.
15. Provider digest controlling an Effect matches the durable binding.

## 14. Required test matrix

Normative coverage lives in `core/effect_acceptance_matrix_test.go` (`TestMatrix*`)
plus barrier/outbox/journal unit tests.

- Ensure response lost after job creation; retry returns same job.
- Delete/supersede before late Ensure success; no leaked reference.
- Unknown Unbound retries Ensure; Unknown Bound observes.
- Same external revision across multiple polls yields every fresh Attempt.
- Wrong Attempt/Poll/Effect/Reference identity rejected.
- Own control crosses barrier; direct/new effect does not.
- Same artifact supersession reuses; changed artifact releases.
- StillReferenced never cancels; last-reference cancellation observed terminally.
- Cancel/complete race; service outage; authoritative Gone.
- Core crash in every Effect/Reference/Control state.
- Outbox duplicate and Journal idempotency.
- Deletion tombstone restart and fail-closed administrator path.

## 15. Risks and remaining production decisions

Not blockers for the in-memory vertical slice:

- final Core execution package layout;
- production SQL schema/transaction and outbox leases;
- provider-version retention duration;
- terminal effect/reference/control compaction policy;
- administrator override authorization/audit API;
- real service transport, auth, and worker fencing.

These must be reviewed before a production backend.

## 16. Design-drift control

For every implementation commit:

- cite phase and invariants;
- add acceptance-gate tests;
- compile normative contract and repository;
- do not add compatibility paths that weaken invariants;
- update/review design before divergent code;
- request independent read-only review after each phase.
