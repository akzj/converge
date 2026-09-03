# Converge Observability: Technical Design

- Status: Proposed for implementation
- Owner: Converge Core
- Last updated: 2026-09-03
- Related: [External Active Effects](external-active-effects.md)

## 1. Purpose and authority

This document defines how Converge exposes enough execution evidence for an
operator or AI Agent to explain what happened without making observability part
of reconciliation correctness.

It is normative for signal semantics, correlation, state-transition ordering,
cardinality, backpressure, and acceptance tests. Concrete OpenTelemetry SDK,
logging backend, and transport configuration belong to the embedding runtime.

Converge owns the meaning of its internal lifecycle. The embedding runtime owns
resource identity such as Agent, site, process, service, and OTLP destination.

## 2. Goals and non-goals

Goals:

- explain Desired acceptance, planning, execution, external effects, recovery,
  verification, deletion, and convergence;
- correlate one logical change across Veyra, Converge Core, and Providers;
- expose committed lifecycle transitions independently of diagnostic messages;
- keep reconciliation correct and responsive when telemetry is slow or down;
- provide bounded-cardinality metrics suitable for alerting and capacity work;
- support redaction and avoid copying Provider payloads into telemetry.

Non-goals:

- telemetry is not a source of truth, execution journal, audit log, or recovery
  mechanism;
- telemetry does not promise exactly-once delivery;
- this design does not choose a production observability backend;
- this design does not define AI authorization or evidence-query APIs;
- this design does not export complete Desired specs, Provider inputs, observed
  properties, environment variables, command output, or secrets.

## 3. Signal boundaries

| Signal | Question answered | Durability | Typical content |
|---|---|---|---|
| Converge state/store | What is true now? | Required | accepted Desired, Plan, Attempt, Effect, control |
| Journal/audit | Which business transition was committed? | Required where contract says so | stable identity, old/new state, result |
| Trace | Why and where did one bounded activity spend time? | Best effort | causal path, timings, retries, Provider calls |
| Log | What diagnostic detail explains this event? | Best effort | structured context, code, reason, error |
| Metric | Is the system healthy in aggregate? | Best effort | rate, duration, queue depth, current counts |

The state store and required journal/outbox remain authoritative. Missing traces,
logs, or metrics must never make a transition ambiguous to recovery.

## 4. Architecture

```text
Veyra Desired + causal context
             |
             v
       Converge Core  ------ context ------> Provider
             |
             | 1. durable CAS / state change
             | 2. committed transition
             v
       Observability contract
        |        |         |
      trace     logs     metrics
        \        |        /
         bounded local SDK/export queue
                    |
                    v
          embedding runtime / OTLP
```

Instrumentation has two inputs:

1. **Activity lifecycle** surrounds bounded work and produces spans.
2. **Committed transitions** describe durable state changes and produce span
   events, structured logs, counters, and runtime accounting.

The implementation must provide a no-op observer. Core tests and embedders do
not need an observability backend.

## 5. Core observability contract

The implementation should place the contract in a small package that imports
only the standard library and Converge model types. The exact package name may
follow repository conventions, but Core must depend on the interface rather
than a concrete OpenTelemetry exporter.

```go
type Observer interface {
	Start(ctx context.Context, activity Activity) (context.Context, Span)
	Committed(ctx context.Context, transition Transition)
	Runtime(ctx context.Context, snapshot RuntimeSnapshot)
}

type Span interface {
	Event(name string, fields ...Field)
	Error(err error, fields ...Field)
	End(result ActivityResult)
}

type Activity struct {
	Kind       ActivityKind
	ConfigID   model.ConfigID
	PlanID     model.PlanID
	Generation model.Generation
	Operation  model.OperationKey
	AttemptID  model.AttemptID
	Provider   string
	Phase      model.Phase
	Cause      CausalContext
}

type Transition struct {
	ID                string
	Kind              TransitionKind
	ExecutionRevision uint64
	At                time.Time

	ConfigID   model.ConfigID
	PlanID     model.PlanID
	Generation model.Generation
	Operation  model.OperationKey
	AttemptID  model.AttemptID
	EffectID    string
	ReferenceID string
	ControlID   string

	Provider string
	Phase    model.Phase
	From     string
	To       string
	Outcome  string
	Code     string
	Reason   string
	Cause    CausalContext
}

type RuntimeSnapshot struct {
	At              time.Time
	ConfigsByState  map[model.ConfigStatus]int64
	AttemptsByState map[model.AttemptStatus]int64
	ControlsByKind  map[string]int64
	OutboxDepth     int64
	PendingPlans    int64
}
```

Effect, Reference, and Control identities are strings at this diagnostic
boundary so an observability package does not import `core` and create a cycle.
They remain named fields and must not be collapsed with the rest of the contract
into an unstructured `map[string]any`. Domain validation continues to use the
typed Core identities before constructing a Transition.

`NoopObserver` must allocate minimally and return the input context with a no-op
Span. Observer construction is explicit through `NewReconciler` options or a
dependency struct. Global loggers and global tracer/meter providers are not the
Core contract.

### 5.1 Transition identity

`Transition.ID` is stable for one committed logical transition. It should be
derived from the durable identity and target execution revision, not generated
anew on every export attempt. Stable identity permits backend deduplication but
does not create an exactly-once guarantee.

Examples:

```text
config/<config>/revision/<revision>/desired-accepted
attempt/<attempt-id>/running
control/<control-id>/revision/<revision>/yielded
effect/<effect-id>/revision/<external-revision>/<state>
```

### 5.2 Commit ordering

For a durable state transition, ordering is mandatory:

```text
validate -> build next snapshot -> CommitExecutionCAS -> Committed
```

Core must never call `Committed` from inside a CAS retry closure and must never
emit a successful state transition before the CAS succeeds. A stale or rejected
CAS may be represented as an activity result or contention metric, but not as a
committed state transition.

If the process crashes after the CAS and before `Committed`, telemetry may be
missing. Recovery correctness comes from the store. Do not add a telemetry
outbox merely to close this gap. Required audit events continue to use the
durable journal/outbox.

### 5.3 Observer failure and latency

Observer methods do not return errors because Core has no correct recovery action
for diagnostic export failure. Implementations must:

- enqueue locally without waiting for network I/O;
- use bounded memory;
- drop or sample telemetry when the queue is full;
- increment `converge.telemetry.dropped` when possible;
- never panic into Core;
- finish shutdown flushing within an embedding-runtime-defined deadline.

An implementation may perform small synchronous SDK bookkeeping. Export, DNS,
connection establishment, retry, compression, and disk spooling must not occur
on a reconciliation path.

## 6. Causal context

```go
type CausalContext struct {
	TraceParent   string
	TraceState    string
	CorrelationID string
	CausationID   string
}
```

- `TraceParent` and `TraceState` use W3C Trace Context wire formats.
- `CorrelationID` is stable for one logical Desired workflow across retries,
  replans, reconnects, and process restart.
- `CausationID` identifies the immediate message, event, or committed transition
  that caused the current activity.

Causality is metadata. It is excluded from Desired digest, Provider digest,
Operation fingerprint, effect semantic fingerprint, and compatibility checks.

`context.Context` is sufficient only for synchronous calls. Converge performs
background work and recovers after restart, so causal metadata must also be
persisted with durable work:

- accepted Desired/Plan cause;
- Attempt cause;
- EffectControl cause;
- outbox Event cause.

An Effect can be shared by multiple plan references and therefore must not own a
single workflow cause. Put causality on the Reference, control, or Attempt that
performed the action.

Long waits must not create spans lasting hours or days. Each activation, replan,
Attempt, control poll, recovery decision, and verification is a separate bounded
trace. A resumed or delayed activity starts a new trace and uses an OpenTelemetry
link to the persisted originating `TraceParent`. It also carries the stable
CorrelationID. Do not reconstruct a false live parent/child relationship after
a process restart.

Provider calls receive the context returned by `Observer.Start`, allowing a
Provider or downstream RPC client to create child spans and propagate W3C trace
headers.

## 7. Stable attributes

All signal implementations use the same names where applicable:

| Attribute | Meaning |
|---|---|
| `converge.config.id` | managed configuration name |
| `converge.plan.id` | immutable Plan identity |
| `converge.generation` | Plan generation |
| `converge.operation.key` | stable operation identity |
| `converge.attempt.id` | concrete Provider attempt |
| `converge.effect.id` | durable external effect |
| `converge.reference.id` | plan ownership reference |
| `converge.control.id` | durable control request |
| `converge.provider.type` | Provider type |
| `converge.provider.digest` | Provider implementation digest |
| `converge.execution.kind` | direct/ensure/observe/release |
| `converge.phase` | prepare/wait/commit/verify/cleanup |
| `converge.from_state` | previous state when applicable |
| `converge.to_state` | committed state when applicable |
| `converge.outcome` | normalized result |
| `converge.code` | stable machine-readable result code |
| `converge.execution.revision` | committed ExecutionSnapshot revision |
| `correlation.id` | logical workflow identity |
| `causation.id` | immediate durable cause |

The embedding runtime adds its own resource attributes, for example Agent ID,
site ID, service name, process identity, and deployment version. Converge Core
must not invent those values.

## 8. Trace model

### 8.1 Bounded activity names

The initial implementation must instrument:

| Span name | Starts | Ends |
|---|---|---|
| `converge.accept_desired` | before validation/durable acceptance | accepted, duplicate, stale, rejected, error |
| `converge.replan` | before Inspect/Replan | Plan CAS outcome |
| `converge.execute_attempt` | after durable Attempt creation | durable attempt result/outbox transition |
| `converge.provider.execute` | immediately before Provider Execute | Provider return/timeout/cancel |
| `converge.effect_control` | after control claim | durable yield/completion/stale/error |
| `converge.provider.ensure_effect` | before EnsureEffect | Provider return/timeout |
| `converge.provider.observe_effects` | before batch observe | Provider return/timeout |
| `converge.provider.ensure_reference` | before EnsureReference | Provider return/timeout |
| `converge.provider.release_effect` | before ReleaseEffect | Provider return/timeout |
| `converge.verify` | before Provider Verify | RecordedState outcome |
| `converge.recover` | Run recovery begins | restoration and reconciliation scheduling complete/error |
| `converge.delete` | delete request is handled | tombstone/final deletion outcome |

The orchestration span and Provider span are separate. This distinguishes Core
queueing/persistence time from external call latency.

### 8.2 Span status and events

- expected states such as Waiting, duplicate, stale list entry, and safe
  cancellation are not span errors;
- timeout, protocol violation, authoritative failure, persistence failure, and
  unexpected Provider error set error status;
- retryable outcomes record `retryable=true` but do not hide the error;
- committed transitions may be attached as span events named
  `converge.state.committed`;
- raw Provider payloads and full command output are never span attributes.

Batch `ObserveEffects` has one transport span. Each result is a span event with
PollRequestID/AttemptID, or a child processing span only when result processing
is materially expensive. Avoid one exported span per routine successful poll if
sampling or events provide sufficient evidence.

### 8.3 Sampling

Head sampling may retain a fraction of routine successful traces. The following
must be eligible for tail/error retention by the runtime:

- Failed, Unknown, timeout, protocol violation, and permanent rejection;
- unusually slow Provider calls or CAS contention;
- recovery decisions and administrator overrides;
- traces explicitly selected by correlation or diagnostic request.

Sampling must not change state transitions or log/metric production.

## 9. Structured logging

Core currently uses process-global `zap.L()` calls. The implementation should
replace Core's global dependency with an injected structured logger or implement
logging behind Observer. Provider-owned logging may use its own facade but must
extract trace and correlation fields from context.

Every diagnostic record includes the applicable stable attributes and current
`trace_id`/`span_id`. Messages remain short; identity is carried in fields.

Levels:

- **Debug**: normal scheduling decisions, duplicate/stale transitions, ordinary
  control polling; disabled or sampled in production.
- **Info**: process recovery summary, Desired accepted, convergence reached,
  deletion completed. Do not log every successful poll.
- **Warn**: retryable Provider failure, repeated CAS contention, telemetry drop,
  delayed control, recoverable outbox failure.
- **Error**: persistence failure, invalid transition/protocol response,
  permanent Provider failure, unresolved Unknown requiring intervention.

`Reason` is diagnostic and may be redacted or truncated. `Code` is stable and
safe for grouping. Stack traces are appropriate for unexpected internal errors,
not normal Provider outcomes.

Rate-limit repeated logs by `(code, provider, operation kind)`. Suppression must
produce a counter so silence cannot be mistaken for recovery.

## 10. Metrics

Metric names use OpenTelemetry dot notation. Backends may translate names.

### 10.1 Counters and histograms

| Metric | Type | Unit | Allowed attributes |
|---|---|---|---|
| `converge.desired.accepted` | counter | `{change}` | `outcome` |
| `converge.plan.created` | counter | `{plan}` | `provider`, `outcome` |
| `converge.plan.duration` | histogram | `s` | `provider`, `outcome` |
| `converge.attempt.started` | counter | `{attempt}` | `provider`, `phase`, `execution_kind` |
| `converge.attempt.completed` | counter | `{attempt}` | `provider`, `phase`, `execution_kind`, `outcome`, `retryable` |
| `converge.attempt.duration` | histogram | `s` | same as completed |
| `converge.provider.call.duration` | histogram | `s` | `provider`, `method`, `outcome` |
| `converge.effect.transition` | counter | `{transition}` | `provider`, `from_state`, `to_state` |
| `converge.control.claim` | counter | `{claim}` | `kind`, `outcome` |
| `converge.cas.conflict` | counter | `{conflict}` | `operation` |
| `converge.recovery.result` | counter | `{item}` | `kind`, `outcome` |
| `converge.outbox.publish` | counter | `{event}` | `outcome` |
| `converge.telemetry.dropped` | counter | `{item}` | `signal`, `reason` |
| `converge.log.suppressed` | counter | `{record}` | `level`, `code` |

Durations use monotonic elapsed time measured in process. Wall-clock timestamps
remain diagnostic fields and must not be subtracted across machines.

### 10.2 Observable gauges

| Metric | Type | Allowed attributes |
|---|---|---|
| `converge.configs` | gauge | `state`, `provider` |
| `converge.attempts.active` | gauge | `state`, `provider`, `phase` |
| `converge.controls.due` | gauge | `kind` |
| `converge.controls.inflight` | gauge | `kind` |
| `converge.outbox.depth` | gauge | none |
| `converge.plans.pending` | gauge | none |
| `converge.oldest.control.age` | gauge | `kind` |
| `converge.oldest.outbox.age` | gauge | none |

Runtime gauges are derived periodically from a consistent in-memory snapshot or
durable registry snapshot. Do not hold `Reconciler.mu`, registry locks, or a
database transaction while invoking an SDK callback or exporter. Snapshot under
the owning lock, release it, then call `Observer.Runtime`.

### 10.3 Cardinality policy

Metric attributes must not contain ConfigID, PlanID, OperationKey, AttemptID,
EffectID, ReferenceID, ControlRequestID, external job ID, digest, correlation ID,
error text, file path, URL, host name, or user-provided Action.

`provider`, `method`, `phase`, `kind`, `state`, `outcome`, `retryable`, and stable
bounded `code` are allowed after validation. Unknown Provider-supplied codes are
normalized to `other` for metrics while the original code remains available in
logs/traces.

Metrics may attach a trace exemplar containing the current trace ID. This is the
preferred path from an aggregate alert to one concrete execution.

## 11. Instrumentation points and durable truth

| Code path | Activity | Committed transitions / observations |
|---|---|---|
| `SubmitDesired` / `AcceptDesired` | accept Desired | accepted revision, duplicate/stale/rejected |
| `planLatest` | Inspect + Replan + Plan CAS | generation installed, CAS contention, plan rejected |
| `executeReady` | admission/claim | Attempt created/running, blocked dependency, arbiter wait |
| `executeAttempt` | Provider Execute | Waiting/Completed/Failed/Cancelled/Unknown committed |
| `runOutboxDispatcher` | publish | publish result, outbox depth/age |
| `handleEvent` | apply event | node/attempt transition and retry scheduling |
| `processOneDueControl` | claim/control | claimed, yielded, stale, completed |
| effect control methods | Provider RPC | Effect/Reference/Control committed transition |
| `verifyAndRecord` | Verify + record | converged/error and RecordedState update |
| `recover` | recovery | restored counts, reclaimed work, unresolved Unknown |
| deletion paths | delete | tombstone, reference release, final deletion |

Instrumentation must preserve the existing continuous reconciliation model:
stable work continues, compatible work may be carried to a newer generation,
and changed work may be replaced. A carried Attempt keeps its AttemptID and
original start time; the carry transition records the new Generation. It must
not be counted as a new Attempt start.

Waiting is a first-class non-terminal result. Measure active waiting work and
wait duration without leaving an execution span open. Each reevaluation or poll
is a new bounded activity linked by durable identity.

## 12. Provider contract

Providers participate through the existing `context.Context` on Inspect,
Replan, EvaluateCondition, Execute, Verify, and EffectProvider methods.

Provider requirements:

- propagate the W3C trace context on outbound calls;
- create child spans around material external operations;
- use stable method/action names, never raw user input as span or metric names;
- return stable `Code` values for grouping;
- keep secrets, full payloads, and unbounded output out of telemetry;
- distinguish timeout, context cancellation, unknown outcome, authoritative
  rejection, and protocol violation;
- do not treat span/log export success as part of Provider success.

Provider-specific metrics use a provider namespace and follow the same
cardinality restrictions. Converge Core metrics must remain useful even when a
Provider emits no telemetry of its own.

## 13. Privacy, security, and size limits

The implementation must centrally enforce:

- an allowlist of attribute keys;
- maximum Reason/log message length;
- maximum event count per span;
- maximum collection length rendered into a log;
- redaction of credentials, tokens, environment values, query parameters, and
  Provider-declared sensitive fields;
- no raw `Desired.Spec`, `Operation.Input`, `Condition.Input`,
  `ObservedState.Properties`, `EnsureSpec`, or command stdout/stderr by default.

Digests may be recorded in traces/logs when needed, but not metrics. An explicit
diagnostic evidence subsystem may retain bounded command output under separate
authorization; observability must reference that evidence by ID rather than copy
it into every signal.

## 14. Veyra integration

The Converge adapter in Veyra supplies:

- W3C context plus CorrelationID/CausationID when activating Desired;
- Agent/site/session identity as OpenTelemetry Resource attributes;
- concrete tracer, meter, logger, sampler, redactor, and bounded processors;
- an OTLP exporter connected to Veyra's dedicated telemetry data path.

Suggested attribute mapping:

```text
Converge config ID        -> veyra.resource.key
Converge operation key    -> veyra.operation.id
Converge attempt ID       -> veyra.attempt.id
Converge provider type    -> veyra.provider.type
Veyra snapshot ID         -> veyra.desired.snapshot.id
Veyra desired revision    -> veyra.desired.revision
```

The adapter may add both names during migration. Converge's contract remains
standalone and must not import Veyra packages.

Telemetry uses a dedicated bounded queue/connection or standard OTLP path. It
must not share flow-control capacity needed by heartbeat, Desired activation,
status, cancellation, or other command traffic.

## 15. Failure behavior

| Failure | Required behavior |
|---|---|
| Observer is nil | install NoopObserver |
| local telemetry queue full | drop/sample, account, continue Core |
| exporter unavailable | bounded retry/backoff outside Core |
| malformed incoming trace context | start a new trace, retain valid correlation ID, warn once/rate-limited |
| Observer panic | recover at adapter boundary, account if possible, continue Core |
| process crashes after state CAS | state remains correct; telemetry may be missing |
| duplicate transition callback | backend may deduplicate by Transition.ID; metrics tolerate documented at-least-once behavior |
| runtime snapshot collection fails | retain prior backend point; do not block reconciliation |
| shutdown deadline expires | abandon telemetry flush and allow process shutdown |

No telemetry failure may cause an Attempt to become Failed, extend an effect
lease, retain a destructive-operation arbiter lock, prevent an outbox ack, or
change a retry decision.

## 16. Implementation sequence

1. Add the no-op contract, typed fields, constructor injection, and unit tests.
2. Add durable causal metadata without changing digests or fingerprints.
3. Instrument activity spans around Provider and persistence boundaries.
4. Return committed transition descriptions from registry mutation methods and
   emit them only after successful CAS.
5. Replace global Core logging with context-aware injected logging.
6. Add bounded metrics and runtime snapshot collection.
7. Add the OpenTelemetry adapter and Veyra mapping outside Converge Core.
8. Add redaction, sampling, overload, and shutdown tests.

Each step must preserve a working no-op configuration and existing lifecycle
tests. Do not combine observability work with retry-policy or state-machine
changes.

## 17. Acceptance gates

### 17.1 Correctness tests

- no committed-transition event is emitted for a failed or stale CAS;
- one successful transition carries the exact committed execution revision;
- duplicate callbacks have stable Transition.ID;
- causality survives store restore and appears on recovered activity links;
- carrying an Attempt to a new generation does not count a new Attempt start;
- Effect controls from different References retain their own causal context;
- Waiting/polling creates bounded spans rather than one long-lived span;
- Provider context contains the child trace context;
- payload and secret fixtures do not appear in exported signals;
- nil/no-op observer preserves all existing behavior.

### 17.2 Fault and performance tests

- an exporter that blocks forever does not block SubmitDesired, Run scheduling,
  Provider execution, cancellation, or shutdown beyond the flush deadline;
- queue saturation stays within its configured memory bound and increments the
  drop counter;
- Observer panic does not escape into Core;
- high-rate polling does not create unbounded logs, spans, or metric series;
- metrics expose no forbidden high-cardinality attributes;
- race tests pass while transitions and runtime snapshots are collected;
- benchmarks compare no-op and enabled instrumentation overhead on planning,
  Attempt completion, and control polling.

The implementation report must state measured overhead and queue limits. A short
smoke run is not evidence of sustained bounded memory; include a repeatable load
test long enough to exercise export backpressure and control polling.

## 18. Operational minimum dashboard and alerts

The first dashboard should show:

- Desired acceptance and convergence rate;
- p50/p95/p99 planning, Attempt, and Provider-call latency;
- configs and Attempts by state;
- due/in-flight EffectControls and oldest age;
- outbox depth, oldest age, and publish failures;
- CAS conflicts and recovery results;
- telemetry dropped/suppressed counts.

Initial alert candidates are sustained growth of outbox/control age, unresolved
Unknown or Failed state, Provider error ratio, reconciliation tail latency, and
telemetry drops. Alerts must link to filtered logs/traces using provider,
time range, and exemplars rather than metric labels containing object IDs.
