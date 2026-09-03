package core

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/akzj/converge/observability"
	"github.com/akzj/converge/pkg/model"
)

type recordingObserver struct {
	mu          sync.Mutex
	transitions []observability.Transition
	runtimes    []observability.RuntimeSnapshot
}

func (*recordingObserver) Start(ctx context.Context, _ observability.Activity) (context.Context, observability.Span) {
	return ctx, recordingSpan{}
}
func (o *recordingObserver) Committed(_ context.Context, transition observability.Transition) {
	o.mu.Lock()
	o.transitions = append(o.transitions, transition)
	o.mu.Unlock()
}
func (o *recordingObserver) Runtime(_ context.Context, snapshot observability.RuntimeSnapshot) {
	o.mu.Lock()
	o.runtimes = append(o.runtimes, snapshot)
	o.mu.Unlock()
}
func (o *recordingObserver) snapshot() []observability.Transition {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]observability.Transition(nil), o.transitions...)
}

type recordingSpan struct{}

func (recordingSpan) Event(string, ...observability.Field) {}
func (recordingSpan) Error(error, ...observability.Field)  {}
func (recordingSpan) End(observability.ActivityResult)     {}

type countingObserver struct{ committed atomic.Uint64 }

func (*countingObserver) Start(ctx context.Context, _ observability.Activity) (context.Context, observability.Span) {
	return ctx, recordingSpan{}
}
func (o *countingObserver) Committed(context.Context, observability.Transition)  { o.committed.Add(1) }
func (*countingObserver) Runtime(context.Context, observability.RuntimeSnapshot) {}

func TestCommittedTransitionRequiresSuccessfulCASAndUsesCommittedRevision(t *testing.T) {
	store := &failingExecutionStore{inner: NewMemoryExecutionStore(), fail: true}
	observer := &recordingObserver{}
	registry := NewPlanRegistry(store)
	registry.observer = observability.Safe(observer)
	desired := model.DesiredState{ConfigID: model.ConfigID{Name: "config"}, ProviderType: "test", Version: 1, Spec: []byte(`{"v":1}`)}
	desired.Digest = model.DesiredSpecDigest(desired.Spec)
	if _, err := registry.AcceptDesired(context.Background(), desired); err == nil {
		t.Fatal("expected failed CAS")
	}
	if got := observer.snapshot(); len(got) != 0 {
		t.Fatalf("failed CAS emitted transitions: %#v", got)
	}
	store.fail = false
	if accepted, err := registry.AcceptDesired(context.Background(), desired); err != nil || !accepted {
		t.Fatalf("accept desired: accepted=%v err=%v", accepted, err)
	}
	transitions := observer.snapshot()
	if len(transitions) != 1 || transitions[0].ExecutionRevision != 1 || transitions[0].ID != "config/config/revision/1/desired-accepted" {
		t.Fatalf("unexpected transition: %#v", transitions)
	}
}

func TestCarriedAttemptDoesNotEmitSecondStart(t *testing.T) {
	observer := &recordingObserver{}
	registry := NewPlanRegistry(NewMemoryExecutionStore())
	registry.observer = observability.Safe(observer)
	first, _, err := registry.Install(context.Background(), 0, testPlan(t, "digest", model.Operation{Key: "apply", ExecutionKind: model.ExecutionDirect, Action: "same"}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.StartAttempt(context.Background(), first.ConfigID, first.Generation, "apply", "attempt-1"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := registry.Install(context.Background(), 1, testPlan(t, "digest", model.Operation{Key: "apply", ExecutionKind: model.ExecutionDirect, Action: "same"}, model.Operation{Key: "new", Action: "add"})); err != nil {
		t.Fatal(err)
	}
	starts, carries := 0, 0
	for _, transition := range observer.snapshot() {
		switch transition.Kind {
		case observability.TransitionAttemptStarted:
			starts++
		case observability.TransitionAttemptCarried:
			carries++
		}
	}
	if starts != 1 || carries != 1 {
		t.Fatalf("starts=%d carries=%d transitions=%#v", starts, carries, observer.snapshot())
	}
}

func TestCausalContextSurvivesSQLiteRestore(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "cause.db")
	store, err := OpenSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	desired := model.DesiredState{
		ConfigID: model.ConfigID{Name: "config"}, ProviderType: "test", Version: 1,
		Spec: []byte(`{"v":1}`), Cause: model.CausalContext{TraceParent: "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01", CorrelationID: "workflow", CausationID: "snapshot-1"},
	}
	desired.Digest = model.DesiredSpecDigest(desired.Spec)
	registry := NewPlanRegistry(store)
	if accepted, err := registry.AcceptDesired(ctx, desired); err != nil || !accepted {
		t.Fatalf("accept desired: %v %v", accepted, err)
	}
	candidate, err := BuildCandidate(desired.ConfigID, desired, desired.ProviderType, "provider-digest", []model.Operation{{Key: "apply", ExecutionKind: model.ExecutionDirect}})
	if err != nil {
		t.Fatal(err)
	}
	installed, _, err := registry.Install(ctx, 0, candidate)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.StartAttempt(ctx, installed.ConfigID, installed.Generation, "apply", "attempt"); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenSQLite(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	restored := NewPlanRegistry(reopened)
	if err := restored.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	got, ok := restored.AcceptedDesired(desired.ConfigID)
	if !ok || got.Cause != desired.Cause {
		t.Fatalf("restored cause = %#v, ok=%v", got.Cause, ok)
	}
	execution := restored.Execution(desired.ConfigID)
	if len(execution.Attempts) != 1 || execution.Attempts[0].Cause != desired.Cause {
		t.Fatalf("restored attempt cause = %#v", execution.Attempts)
	}
}

func TestEffectReferencesKeepIndependentCauses(t *testing.T) {
	registry := NewPlanRegistry(NewMemoryExecutionStore())
	plan, _, err := registry.Install(context.Background(), 0, testPlan(t, "digest", model.Operation{Key: "apply", ExecutionKind: model.ExecutionDirect}))
	if err != nil {
		t.Fatal(err)
	}
	for index, correlation := range []string{"workflow-a", "workflow-b"} {
		identity := TransitionIdentity{
			EffectIdentity: EffectIdentity{ConfigID: plan.ConfigID, PlanID: plan.ID, Generation: plan.Generation, OperationKey: "apply", EffectKey: correlation, EffectID: EffectID("effect-" + correlation), ReferenceID: ReferenceID("ref-" + correlation), ProviderType: "test", ProviderDigest: "digest"},
			RequestID:      ControlRequestID("control-" + correlation), Cause: model.CausalContext{CorrelationID: correlation},
		}
		if disposition, err := registry.BeginEnsureEffect(context.Background(), BeginEnsureRequest{Identity: identity, Spec: ImmutableEnsureSpec{IdempotencyKey: correlation, ArtifactID: "artifact", SemanticFingerprint: "fp"}}); err != nil || disposition != TransitionApplied {
			t.Fatalf("begin ensure %d: disposition=%s err=%v", index, disposition, err)
		}
	}
	execution := registry.Execution(plan.ConfigID)
	causes := make(map[ReferenceID]string)
	for _, reference := range execution.EffectReferences {
		causes[reference.ID] = reference.Cause.CorrelationID
	}
	if causes["ref-workflow-a"] != "workflow-a" || causes["ref-workflow-b"] != "workflow-b" {
		t.Fatalf("reference causes mixed: %#v", causes)
	}
}

type childContextKey struct{}

type childContextObserver struct{}

func (childContextObserver) Start(ctx context.Context, activity observability.Activity) (context.Context, observability.Span) {
	if activity.Kind == observability.ActivityReplan {
		ctx = context.WithValue(ctx, childContextKey{}, true)
	}
	return ctx, recordingSpan{}
}
func (childContextObserver) Committed(context.Context, observability.Transition)    {}
func (childContextObserver) Runtime(context.Context, observability.RuntimeSnapshot) {}

type contextCheckingProvider struct {
	*mockProvider
	seen atomic.Bool
}

func (p *contextCheckingProvider) Inspect(ctx context.Context, resource model.ResourceID) (model.ObservedState, error) {
	p.seen.Store(ctx.Value(childContextKey{}) == true)
	return p.mockProvider.Inspect(ctx, resource)
}

func TestProviderReceivesObserverContext(t *testing.T) {
	provider := &contextCheckingProvider{mockProvider: &mockProvider{typeName: "context"}}
	r := NewReconciler(NewMemoryStateStore(), NewMemoryExecutionStore(), NewMemoryEventBus(), NewMemoryArbiter(), NewMemoryJournal(), WithObserver(childContextObserver{}))
	r.RegisterProvider(context.Background(), provider)
	desired := model.DesiredState{ConfigID: model.ConfigID{Name: "config"}, ProviderType: provider.Type(), Version: 1, Spec: []byte(`{"v":1}`)}
	desired.Digest = model.DesiredSpecDigest(desired.Spec)
	if err := r.SubmitDesired(context.Background(), desired); err != nil {
		t.Fatal(err)
	}
	r.planLatest(context.Background(), desired.ConfigID.Name)
	if !provider.seen.Load() {
		t.Fatal("provider did not receive observer child context")
	}
}

func TestRuntimeSnapshotIsAggregateAndDetached(t *testing.T) {
	observer := &recordingObserver{}
	r := NewReconciler(NewMemoryStateStore(), NewMemoryExecutionStore(), NewMemoryEventBus(), NewMemoryArbiter(), NewMemoryJournal(), WithObserver(observer))
	r.mu.Lock()
	r.configs["config"] = &model.ManagedConfig{ID: model.ConfigID{Name: "config"}, Status: model.ConfigConverging}
	r.pendingPlans["config"] = struct{}{}
	r.mu.Unlock()
	r.emitRuntimeSnapshot(context.Background())
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if len(observer.runtimes) != 1 || observer.runtimes[0].ConfigsByState[model.ConfigConverging] != 1 || observer.runtimes[0].PendingPlans != 1 {
		t.Fatalf("runtime snapshots: %#v", observer.runtimes)
	}
}

type lifecycleObserver struct {
	mu      sync.Mutex
	started map[observability.ActivityKind]int
	ended   map[observability.ActivityKind]int
}

func newLifecycleObserver() *lifecycleObserver {
	return &lifecycleObserver{started: make(map[observability.ActivityKind]int), ended: make(map[observability.ActivityKind]int)}
}
func (o *lifecycleObserver) Start(ctx context.Context, activity observability.Activity) (context.Context, observability.Span) {
	o.mu.Lock()
	o.started[activity.Kind]++
	o.mu.Unlock()
	return ctx, &lifecycleSpan{owner: o, kind: activity.Kind}
}
func (*lifecycleObserver) Committed(context.Context, observability.Transition)    {}
func (*lifecycleObserver) Runtime(context.Context, observability.RuntimeSnapshot) {}

type lifecycleSpan struct {
	once  sync.Once
	owner *lifecycleObserver
	kind  observability.ActivityKind
}

func (*lifecycleSpan) Event(string, ...observability.Field) {}
func (*lifecycleSpan) Error(error, ...observability.Field)  {}
func (s *lifecycleSpan) End(observability.ActivityResult) {
	s.once.Do(func() {
		s.owner.mu.Lock()
		s.owner.ended[s.kind]++
		s.owner.mu.Unlock()
	})
}

func TestControlPollingUsesBoundedActivities(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryExecutionStore()
	registry := NewPlanRegistry(store)
	plan, _, err := registry.Install(ctx, 0, testPlan(t, "digest", model.Operation{Key: "apply", ExecutionKind: model.ExecutionDirect}))
	if err != nil {
		t.Fatal(err)
	}
	identity := beginBoundEffect(t, registry, plan, "effect", "reference", "job")
	observer := newLifecycleObserver()
	r := NewReconciler(NewMemoryStateStore(), store, NewMemoryEventBus(), NewMemoryArbiter(), NewMemoryJournal(), WithObserver(observer))
	r.registry = registry
	r.registry.observer = r.observer
	r.providerVersions["test"] = map[string]Provider{"digest": NewFakeDownloadProvider("digest", NewFakeDownloadService())}
	refs, err := registry.ListDueControls(ctx, time.Now())
	if err != nil || len(refs) == 0 {
		t.Fatalf("due controls=%#v err=%v", refs, err)
	}
	if err := r.processOneDueControl(ctx, refs[0], time.Now()); err != nil {
		t.Fatal(err)
	}
	observer.mu.Lock()
	defer observer.mu.Unlock()
	if observer.started[observability.ActivityEffectControl] != 1 || observer.ended[observability.ActivityEffectControl] != 1 {
		t.Fatalf("control activity not bounded: started=%#v ended=%#v identity=%#v", observer.started, observer.ended, identity)
	}
	providerStarts := observer.started[observability.ActivityObserveEffects] + observer.started[observability.ActivityEnsureEffect] + observer.started[observability.ActivityReleaseEffect] + observer.started[observability.ActivityEnsureReference]
	providerEnds := observer.ended[observability.ActivityObserveEffects] + observer.ended[observability.ActivityEnsureEffect] + observer.ended[observability.ActivityReleaseEffect] + observer.ended[observability.ActivityEnsureReference]
	if providerStarts != 1 || providerEnds != 1 {
		t.Fatalf("provider polling activity not bounded: started=%#v ended=%#v", observer.started, observer.ended)
	}
}

func benchmarkObservers() map[string]observability.Observer {
	return map[string]observability.Observer{
		"noop":      observability.Noop(),
		"recording": observability.Safe(&countingObserver{}),
	}
}

func BenchmarkDesiredAcceptanceObservability(b *testing.B) {
	for name, observer := range benchmarkObservers() {
		b.Run(name, func(b *testing.B) {
			registry := NewPlanRegistry()
			registry.observer = observer
			desired := model.DesiredState{ConfigID: model.ConfigID{Name: "config"}, ProviderType: "test", Spec: []byte(`{"v":1}`)}
			desired.Digest = model.DesiredSpecDigest(desired.Spec)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				desired.Version = uint64(i + 1)
				if _, err := registry.AcceptDesired(context.Background(), desired); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkPlanInstallObservability(b *testing.B) {
	for name, observer := range benchmarkObservers() {
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				registry := NewPlanRegistry()
				registry.observer = observer
				plan, err := BuildCandidate(model.ConfigID{Name: "config"}, model.DesiredState{ConfigID: model.ConfigID{Name: "config"}, ProviderType: "test", Version: 1, Digest: "desired"}, "test", "provider", []model.Operation{{Key: "apply", ExecutionKind: model.ExecutionDirect}})
				if err != nil {
					b.Fatal(err)
				}
				if _, _, err := registry.Install(context.Background(), 0, plan); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkAttemptCompletionObservability(b *testing.B) {
	for name, observer := range benchmarkObservers() {
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				registry := NewPlanRegistry()
				registry.observer = observer
				plan, err := BuildCandidate(model.ConfigID{Name: "config"}, model.DesiredState{ConfigID: model.ConfigID{Name: "config"}, ProviderType: "test", Version: 1, Digest: "desired"}, "test", "provider", []model.Operation{{Key: "apply", ExecutionKind: model.ExecutionDirect}})
				if err != nil {
					b.Fatal(err)
				}
				installed, _, err := registry.Install(context.Background(), 0, plan)
				if err != nil {
					b.Fatal(err)
				}
				attempt, err := registry.StartAttempt(context.Background(), installed.ConfigID, installed.Generation, "apply", "attempt")
				if err != nil {
					b.Fatal(err)
				}
				if _, _, err := registry.ApplyEvent(context.Background(), model.Event{PlanID: attempt.PlanID, Generation: attempt.Generation, ConfigID: attempt.ConfigID.Name, NodeKey: attempt.NodeKey, AttemptID: attempt.ID, State: model.StepCompleted}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkControlPollingObservability(b *testing.B) {
	for name, observer := range benchmarkObservers() {
		b.Run(name, func(b *testing.B) {
			registry := NewPlanRegistry()
			registry.observer = observer
			plan, err := BuildCandidate(model.ConfigID{Name: "config"}, model.DesiredState{ConfigID: model.ConfigID{Name: "config"}, ProviderType: "test", Version: 1, Digest: "desired"}, "test", "provider", []model.Operation{{Key: "apply", EffectKey: "slot", ExecutionKind: model.ExecutionEffectEnsure}})
			if err != nil {
				b.Fatal(err)
			}
			installed, _, err := registry.Install(context.Background(), 0, plan)
			if err != nil {
				b.Fatal(err)
			}
			identity := TransitionIdentity{EffectIdentity: EffectIdentity{ConfigID: installed.ConfigID, PlanID: installed.ID, Generation: installed.Generation, OperationKey: "apply", EffectKey: "slot", EffectID: "effect", ReferenceID: "reference", ProviderType: "test", ProviderDigest: "provider"}, RequestID: "control"}
			if _, err := registry.BeginEnsureEffect(context.Background(), BeginEnsureRequest{Identity: identity, Spec: ImmutableEnsureSpec{IdempotencyKey: "key", ArtifactID: "artifact", SemanticFingerprint: "fingerprint"}}); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				now := time.Now()
				if _, err := registry.ClaimDueControl(context.Background(), installed.ConfigID, identity.RequestID, now, "poll-attempt", "poll", now.Add(time.Second)); err != nil {
					b.Fatal(err)
				}
				identity.AttemptID = "poll-attempt"
				if _, err := registry.YieldControl(context.Background(), identity, time.Time{}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
