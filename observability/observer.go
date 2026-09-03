// Package observability defines Converge's backend-neutral telemetry contract.
package observability

import (
	"context"
	"errors"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"github.com/akzj/converge/pkg/model"
)

type ActivityKind string

const (
	ActivityAcceptDesired   ActivityKind = "converge.accept_desired"
	ActivityReplan          ActivityKind = "converge.replan"
	ActivityExecuteAttempt  ActivityKind = "converge.execute_attempt"
	ActivityProviderExecute ActivityKind = "converge.provider.execute"
	ActivityEffectControl   ActivityKind = "converge.effect_control"
	ActivityEnsureEffect    ActivityKind = "converge.provider.ensure_effect"
	ActivityObserveEffects  ActivityKind = "converge.provider.observe_effects"
	ActivityEnsureReference ActivityKind = "converge.provider.ensure_reference"
	ActivityReleaseEffect   ActivityKind = "converge.provider.release_effect"
	ActivityVerify          ActivityKind = "converge.verify"
	ActivityRecover         ActivityKind = "converge.recover"
	ActivityDelete          ActivityKind = "converge.delete"
)

type TransitionKind string

const (
	TransitionDesiredAccepted TransitionKind = "desired_accepted"
	TransitionPlanInstalled   TransitionKind = "plan_installed"
	TransitionAttemptStarted  TransitionKind = "attempt_started"
	TransitionAttemptFinished TransitionKind = "attempt_finished"
	TransitionAttemptCarried  TransitionKind = "attempt_carried"
	TransitionControlChanged  TransitionKind = "control_changed"
	TransitionEffectChanged   TransitionKind = "effect_changed"
	TransitionConverged       TransitionKind = "converged"
	TransitionDeleted         TransitionKind = "deleted"
)

type CausalContext = model.CausalContext

type Field struct {
	Key   string
	Value string
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

type ActivityResult struct {
	Outcome   string
	Code      string
	Reason    string
	Retryable bool
}

type Transition struct {
	ID                string
	Kind              TransitionKind
	ExecutionRevision uint64
	At                time.Time
	ConfigID          model.ConfigID
	PlanID            model.PlanID
	Generation        model.Generation
	Operation         model.OperationKey
	AttemptID         model.AttemptID
	EffectID          string
	ReferenceID       string
	ControlID         string
	Provider          string
	Phase             model.Phase
	From              string
	To                string
	Outcome           string
	Code              string
	Reason            string
	Cause             CausalContext
}

type RuntimeSnapshot struct {
	At              time.Time
	ConfigsByState  map[model.ConfigStatus]int64
	AttemptsByState map[model.AttemptStatus]int64
	ControlsByKind  map[string]int64
	OutboxDepth     int64
	PendingPlans    int64
}

type Observer interface {
	Start(context.Context, Activity) (context.Context, Span)
	Committed(context.Context, Transition)
	Runtime(context.Context, RuntimeSnapshot)
}

type Span interface {
	Event(name string, fields ...Field)
	Error(err error, fields ...Field)
	End(ActivityResult)
}

type NoopObserver struct{}
type noopSpan struct{}

func Noop() Observer { return NoopObserver{} }
func (NoopObserver) Start(ctx context.Context, _ Activity) (context.Context, Span) {
	return ctx, noopSpan{}
}
func (NoopObserver) Committed(context.Context, Transition)    {}
func (NoopObserver) Runtime(context.Context, RuntimeSnapshot) {}
func (noopSpan) Event(string, ...Field)                       {}
func (noopSpan) Error(error, ...Field)                        {}
func (noopSpan) End(ActivityResult)                           {}

// Safe converts observer panics into dropped telemetry rather than Core panics.
func Safe(observer Observer) Observer {
	if observer == nil {
		return Noop()
	}
	return safeObserver{inner: observer}
}

type safeObserver struct{ inner Observer }

func (o safeObserver) Start(ctx context.Context, activity Activity) (next context.Context, span Span) {
	next, span = ctx, noopSpan{}
	defer func() { _ = recover() }()
	next, span = o.inner.Start(ctx, activity)
	if next == nil {
		next = ctx
	}
	if span == nil {
		span = noopSpan{}
	}
	span = SafeSpan(span)
	return
}
func (o safeObserver) Committed(ctx context.Context, transition Transition) {
	defer func() { _ = recover() }()
	o.inner.Committed(ctx, sanitizeTransition(transition))
}
func (o safeObserver) Runtime(ctx context.Context, snapshot RuntimeSnapshot) {
	defer func() { _ = recover() }()
	o.inner.Runtime(ctx, cloneRuntime(snapshot))
}

type safeSpan struct{ inner Span }

func SafeSpan(span Span) Span {
	if span == nil {
		return noopSpan{}
	}
	return safeSpan{inner: span}
}
func (s safeSpan) Event(name string, fields ...Field) {
	defer func() { _ = recover() }()
	s.inner.Event(name, sanitizeFields(fields)...)
}
func (s safeSpan) Error(err error, fields ...Field) {
	defer func() { _ = recover() }()
	if err != nil {
		err = errors.New(sanitizeText(err.Error()))
	}
	s.inner.Error(err, sanitizeFields(fields)...)
}
func (s safeSpan) End(result ActivityResult) {
	defer func() { _ = recover() }()
	result.Reason = sanitizeText(result.Reason)
	s.inner.End(result)
}

const MaxReasonLength = 1024

var allowedFields = map[string]struct{}{
	"outcome": {}, "code": {}, "retryable": {}, "method": {}, "state": {},
	"converge.from_state": {}, "converge.to_state": {}, "converge.execution.revision": {},
}

func sanitizeFields(fields []Field) []Field {
	result := make([]Field, 0, len(fields))
	for _, field := range fields {
		if _, ok := allowedFields[field.Key]; ok {
			field.Value = sanitizeText(field.Value)
			result = append(result, field)
		}
	}
	return result
}

func sanitizeTransition(value Transition) Transition {
	value.Reason = sanitizeText(value.Reason)
	return value
}
func truncate(value string) string {
	if len(value) > MaxReasonLength {
		return value[:MaxReasonLength]
	}
	return value
}

var (
	jsonSecretPattern = regexp.MustCompile(`(?i)("(?:password|passwd|token|secret|authorization|api[_-]?key|credential)"\s*:\s*)"[^"]*"`)
	keySecretPattern  = regexp.MustCompile(`(?i)((?:password|passwd|token|secret|authorization|api[_-]?key|credential)\s*[=:]\s*)[^,\s]+`)
	bearerPattern     = regexp.MustCompile(`(?i)(bearer\s+)[a-z0-9._~+/=-]+`)
	queryPattern      = regexp.MustCompile(`([?&][^=\s&]+)=([^&\s]+)`)
)

func sanitizeText(value string) string {
	if value == "" {
		return ""
	}
	value = jsonSecretPattern.ReplaceAllString(value, `${1}"[REDACTED]"`)
	value = keySecretPattern.ReplaceAllString(value, `${1}[REDACTED]`)
	value = bearerPattern.ReplaceAllString(value, `${1}[REDACTED]`)
	value = queryPattern.ReplaceAllString(value, `${1}[REDACTED]`)
	return truncate(value)
}
func cloneRuntime(value RuntimeSnapshot) RuntimeSnapshot {
	copy := value
	copy.ConfigsByState = cloneMap(value.ConfigsByState)
	copy.AttemptsByState = cloneMap(value.AttemptsByState)
	copy.ControlsByKind = cloneMap(value.ControlsByKind)
	return copy
}
func cloneMap[K comparable](source map[K]int64) map[K]int64 {
	result := make(map[K]int64, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

// AsyncSink receives bounded, sanitized telemetry away from Core goroutines.
type AsyncSink interface {
	Activity(context.Context, CompletedActivity)
	Committed(context.Context, Transition)
	Runtime(context.Context, RuntimeSnapshot)
}

type CompletedActivity struct {
	Activity  Activity
	StartedAt time.Time
	EndedAt   time.Time
	Result    ActivityResult
	Events    []SpanEvent
}
type SpanEvent struct {
	Name   string
	At     time.Time
	Fields []Field
	Error  string
}

type AsyncObserver struct {
	sink      AsyncSink
	queue     chan any
	queueMu   sync.RWMutex
	closed    bool
	dropped   atomic.Uint64
	done      chan struct{}
	once      sync.Once
	maxEvents int
}

const (
	DefaultQueueSize = 1024
	DefaultMaxEvents = 32
)

func NewAsync(sink AsyncSink, queueSize, maxEvents int) *AsyncObserver {
	if queueSize < 1 {
		queueSize = DefaultQueueSize
	}
	if maxEvents < 1 {
		maxEvents = DefaultMaxEvents
	}
	o := &AsyncObserver{sink: sink, queue: make(chan any, queueSize), done: make(chan struct{}), maxEvents: maxEvents}
	go o.run()
	return o
}
func (o *AsyncObserver) Start(ctx context.Context, activity Activity) (context.Context, Span) {
	return ctx, &asyncSpan{owner: o, activity: activity, startedAt: time.Now()}
}
func (o *AsyncObserver) Committed(_ context.Context, transition Transition) {
	o.enqueue(sanitizeTransition(transition))
}
func (o *AsyncObserver) Runtime(_ context.Context, snapshot RuntimeSnapshot) {
	o.enqueue(cloneRuntime(snapshot))
}
func (o *AsyncObserver) Dropped() uint64 { return o.dropped.Load() }
func (o *AsyncObserver) enqueue(value any) {
	o.queueMu.RLock()
	defer o.queueMu.RUnlock()
	if o.closed {
		o.dropped.Add(1)
		return
	}
	select {
	case o.queue <- value:
	default:
		o.dropped.Add(1)
	}
}
func (o *AsyncObserver) run() {
	defer close(o.done)
	for value := range o.queue {
		func() {
			defer func() {
				if recover() != nil {
					o.dropped.Add(1)
				}
			}()
			switch item := value.(type) {
			case CompletedActivity:
				o.sink.Activity(context.Background(), item)
			case Transition:
				o.sink.Committed(context.Background(), item)
			case RuntimeSnapshot:
				o.sink.Runtime(context.Background(), item)
			}
		}()
	}
}
func (o *AsyncObserver) Shutdown(ctx context.Context) error {
	o.once.Do(func() {
		o.queueMu.Lock()
		o.closed = true
		close(o.queue)
		o.queueMu.Unlock()
	})
	select {
	case <-o.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type asyncSpan struct {
	mu        sync.Mutex
	owner     *AsyncObserver
	activity  Activity
	startedAt time.Time
	events    []SpanEvent
	ended     bool
}

func (s *asyncSpan) Event(name string, fields ...Field) {
	s.add(SpanEvent{Name: name, At: time.Now(), Fields: sanitizeFields(fields)})
}
func (s *asyncSpan) Error(err error, fields ...Field) {
	message := ""
	if err != nil {
		message = sanitizeText(err.Error())
	}
	s.add(SpanEvent{Name: "error", At: time.Now(), Fields: sanitizeFields(fields), Error: message})
}
func (s *asyncSpan) add(event SpanEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.ended && len(s.events) < s.owner.maxEvents {
		s.events = append(s.events, event)
	} else if !s.ended {
		s.owner.dropped.Add(1)
	}
}
func (s *asyncSpan) End(result ActivityResult) {
	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return
	}
	s.ended = true
	events := append([]SpanEvent(nil), s.events...)
	s.mu.Unlock()
	result.Reason = sanitizeText(result.Reason)
	s.owner.enqueue(CompletedActivity{Activity: s.activity, StartedAt: s.startedAt, EndedAt: time.Now(), Result: result, Events: events})
}
