package core

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"slices"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/akzj/converge/pkg/model"
)

const (
	maxConcurrentExecutions = 10
	maxConcurrentControls   = 10
	maxConcurrentPlans      = 10
	maxConcurrentDeletes    = 4
	providerPlanTimeout     = 30 * time.Second
	providerExecuteTimeout  = 30 * time.Minute
)

// Reconciler owns desired state and drives generation-aware plan execution.
type Reconciler struct {
	mu sync.RWMutex
	// submitMu serializes dependency validation with durable desired acceptance.
	submitMu sync.Mutex

	providers        map[string]Provider
	providerVersions map[string]map[string]Provider
	store            StateStore
	events           EventBus
	arbiter          Arbiter
	journal          Journal
	registry         *PlanRegistry

	configs      map[string]*model.ManagedConfig
	cancels      map[model.AttemptID]context.CancelFunc
	pendingPlans map[string]struct{}
	planning     map[string]bool

	desiredWake    chan struct{}
	pendingDelete  chan deleteRequest
	execSem        chan struct{}
	controlSem     chan struct{}
	planSem        chan struct{}
	controlScanSem chan struct{}
	outboxWake     chan struct{}
	controlWake    chan struct{}
	controlTimeout time.Duration
	planTimeout    time.Duration
	executeTimeout time.Duration
}

func NewReconciler(store StateStore, executionStore ExecutionStore, events EventBus, arbiter Arbiter, journal Journal) *Reconciler {
	return &Reconciler{
		providers: make(map[string]Provider), providerVersions: make(map[string]map[string]Provider), store: store, events: events, arbiter: arbiter, journal: journal,
		registry: NewPlanRegistry(executionStore), configs: make(map[string]*model.ManagedConfig), cancels: make(map[model.AttemptID]context.CancelFunc), pendingPlans: make(map[string]struct{}), planning: make(map[string]bool),
		desiredWake: make(chan struct{}, 1), pendingDelete: make(chan deleteRequest, 128), execSem: make(chan struct{}, maxConcurrentExecutions), controlSem: make(chan struct{}, maxConcurrentControls), planSem: make(chan struct{}, maxConcurrentPlans), controlScanSem: make(chan struct{}, 1), outboxWake: make(chan struct{}, 1), controlWake: make(chan struct{}, 1), controlTimeout: effectControlRPCTimeout, planTimeout: providerPlanTimeout, executeTimeout: providerExecuteTimeout,
	}
}

// wakeControls signals the Run loop that due EffectControls may exist, so it
// stops blocking in select and runs processDueControls.
func (r *Reconciler) wakeControls() {
	select {
	case r.controlWake <- struct{}{}:
	default:
	}
}

func (r *Reconciler) RegisterProvider(ctx context.Context, provider Provider) {
	r.mu.Lock()
	old := r.providers[provider.Type()]
	if r.providerVersions[provider.Type()] == nil {
		r.providerVersions[provider.Type()] = make(map[string]Provider)
	}
	r.providerVersions[provider.Type()][provider.Digest()] = provider
	r.providers[provider.Type()] = provider
	var affected []string
	if old == nil || old.Digest() != provider.Digest() {
		for name, config := range r.configs {
			if config.Desired.ProviderType == provider.Type() {
				affected = append(affected, name)
			}
		}
	}
	r.mu.Unlock()
	for _, name := range affected {
		r.queuePlan(name)
	}
}

func (r *Reconciler) SubmitDesired(ctx context.Context, desired model.DesiredState) error {
	if err := validateDesired(desired); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	r.submitMu.Lock()
	defer r.submitMu.Unlock()
	if err := r.validateConfigDependencies(desired); err != nil {
		return err
	}
	accepted, err := r.registry.AcceptDesired(ctx, desired)
	if err != nil {
		return err
	}
	r.mu.Lock()
	current := r.configs[desired.ConfigID.Name]
	shouldPlan := accepted || current == nil || current.Status != model.ConfigConverged
	if current == nil {
		current = &model.ManagedConfig{ID: desired.ConfigID}
		r.configs[desired.ConfigID.Name] = current
	}
	current.Desired = model.CloneDesiredState(desired)
	current.DependsOnConfigs = append([]string(nil), desired.DependsOn...)
	if shouldPlan {
		current.Status = model.ConfigConverging
		current.LastError = ""
		r.pendingPlans[desired.ConfigID.Name] = struct{}{}
	}
	r.mu.Unlock()
	if shouldPlan {
		select {
		case r.desiredWake <- struct{}{}:
		default:
		}
	}
	return nil
}

func validateDesired(desired model.DesiredState) error {
	if desired.ConfigID.Name == "" {
		return errors.New("desired config ID is empty")
	}
	if desired.ProviderType == "" {
		return errors.New("desired provider type is empty")
	}
	if desired.Version == 0 {
		return errors.New("desired version is zero")
	}
	expected := model.DesiredSpecDigest(desired.Spec)
	if desired.Digest != expected {
		return errors.Errorf("desired digest mismatch: got %q, want %q", desired.Digest, expected)
	}
	return nil
}

// Config returns a detached snapshot of the accepted desired and its current
// convergence status.
func (r *Reconciler) Config(name string) (model.ManagedConfig, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	managed := r.configs[name]
	if managed == nil {
		return model.ManagedConfig{}, false
	}
	copy := *managed
	copy.Desired = model.CloneDesiredState(managed.Desired)
	copy.DependsOnConfigs = append([]string(nil), managed.DependsOnConfigs...)
	copy.Observed.Properties = append([]byte(nil), managed.Observed.Properties...)
	return copy, true
}

// ConfigNames returns a stable snapshot of all configurations currently known
// to the reconciler, including configurations still converging or deleting.
func (r *Reconciler) ConfigNames() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.configs))
	for name := range r.configs {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// Refresh schedules a fresh Inspect/Replan cycle without changing Desired.
func (r *Reconciler) Refresh(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	if r.configs[name] == nil {
		r.mu.Unlock()
		return errors.Errorf("config %q not found", name)
	}
	r.pendingPlans[name] = struct{}{}
	r.mu.Unlock()
	select {
	case r.desiredWake <- struct{}{}:
	default:
	}
	return nil
}

func (r *Reconciler) SubmitDelete(ctx context.Context, name string) error {
	return r.submitDelete(ctx, deleteRequest{name: name})
}

// SubmitDeleteIfDesired queues deletion only while the accepted Desired still
// has the supplied identity. It prevents a delayed old-snapshot deletion from
// removing a configuration reintroduced by a newer snapshot.
func (r *Reconciler) SubmitDeleteIfDesired(ctx context.Context, desired model.DesiredState) error {
	return r.submitDelete(ctx, deleteRequest{name: desired.ConfigID.Name, version: desired.Version, digest: desired.Digest, conditional: true})
}

type deleteRequest struct {
	name        string
	version     uint64
	digest      string
	conditional bool
}

func (r *Reconciler) submitDelete(ctx context.Context, request deleteRequest) error {
	select {
	case r.pendingDelete <- request:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Reconciler) Run(ctx context.Context) error {
	return r.run(ctx, nil)
}

// RunWithReady is Run with an explicit recovery barrier for embedding
// runtimes. Exactly one value is sent after recovery succeeds or fails.
func (r *Reconciler) RunWithReady(ctx context.Context, ready chan<- error) error {
	return r.run(ctx, ready)
}

func (r *Reconciler) run(ctx context.Context, ready chan<- error) error {
	if err := r.recover(ctx); err != nil {
		if ready != nil {
			ready <- err
		}
		return errors.Wrap(err, "recover")
	}
	eventCh, err := r.events.Subscribe(ctx, "")
	if err != nil {
		if ready != nil {
			ready <- err
		}
		return errors.Wrap(err, "subscribe")
	}
	go r.runOutboxDispatcher(ctx)
	for range maxConcurrentDeletes {
		go r.runDeleteWorker(ctx)
	}
	if ready != nil {
		ready <- nil
	}
	r.wakeOutbox()
	driftTicker := time.NewTicker(30 * time.Second)
	controlTicker := time.NewTicker(time.Second)
	defer driftTicker.Stop()
	defer controlTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.desiredWake:
			r.planAcceptedDesired(ctx)
		case event := <-eventCh:
			r.handleEvent(ctx, event)
		case <-r.controlWake:
			// EffectControl scheduler may have work; processDueControls runs below.
		case <-driftTicker.C:
			r.detectDrift(ctx)
		case <-controlTicker.C:
			// Periodically wake delayed controls whose NextCheckAt has arrived.
		}
		r.processDueControls(ctx)
		r.executeReady(ctx)
	}
}

func (r *Reconciler) runDeleteWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case request := <-r.pendingDelete:
			r.deleteConfigRequest(ctx, request)
		}
	}
}

func (r *Reconciler) recover(ctx context.Context) error {
	if err := r.registry.Restore(ctx); err != nil {
		return err
	}
	ids, err := r.store.List(ctx)
	if err != nil {
		return err
	}
	r.mu.Lock()
	for _, id := range ids {
		recorded, err := r.store.Get(ctx, id)
		if err != nil {
			return err
		}
		if recorded == nil {
			continue
		}
		r.configs[id.Name] = &model.ManagedConfig{ID: id, Recorded: *recorded, Status: recorded.Status, Desired: model.DesiredState{ConfigID: id, ProviderType: recorded.ProviderType, Version: recorded.DesiredVersion, Digest: recorded.DesiredDigest}}
	}
	// AcceptedDesired is authoritative even when planning previously failed.
	for _, desired := range r.registry.AcceptedDesireds() {
		managed := r.configs[desired.ConfigID.Name]
		if managed == nil {
			managed = &model.ManagedConfig{ID: desired.ConfigID}
			r.configs[desired.ConfigID.Name] = managed
		}
		if desired.Version >= managed.Desired.Version {
			managed.Desired = model.CloneDesiredState(desired)
			managed.DependsOnConfigs = append([]string(nil), desired.DependsOn...)
			managed.Status = model.ConfigConverging
			r.pendingPlans[desired.ConfigID.Name] = struct{}{}
		}
	}
	r.mu.Unlock()
	// Deletions that were already drained can finish without a provider.
	for _, desired := range r.registry.AcceptedDesireds() {
		if r.registry.IsDeleting(desired.ConfigID) && r.registry.DeletionReady(desired.ConfigID) {
			r.finalizeDeletion(ctx, desired.ConfigID)
		}
	}
	select {
	case r.desiredWake <- struct{}{}:
	default:
	}
	return nil
}

func (r *Reconciler) planAcceptedDesired(ctx context.Context) {
	r.mu.Lock()
	var names []string
	for name := range r.pendingPlans {
		if r.planning[name] {
			continue
		}
		select {
		case r.planSem <- struct{}{}:
		default:
			continue
		}
		r.planning[name] = true
		names = append(names, name)
		delete(r.pendingPlans, name)
	}
	r.mu.Unlock()
	for _, name := range names {
		go func(name string) {
			planCtx, cancel := context.WithTimeout(ctx, r.planTimeout)
			defer cancel()
			r.planLatest(planCtx, name)
			r.invalidateDependents(ctx, name)
			<-r.planSem
			r.mu.Lock()
			delete(r.planning, name)
			r.mu.Unlock()
			select {
			case r.desiredWake <- struct{}{}:
			default:
			}
		}(name)
	}
}

func (r *Reconciler) queuePlan(name string) {
	r.mu.Lock()
	if r.configs[name] != nil {
		r.pendingPlans[name] = struct{}{}
	}
	r.mu.Unlock()
	select {
	case r.desiredWake <- struct{}{}:
	default:
	}
}

func (r *Reconciler) scheduleVerify(ctx context.Context, plan *model.Plan, provider Provider) {
	select {
	case r.planSem <- struct{}{}:
		go func() {
			defer func() {
				<-r.planSem
				select {
				case r.desiredWake <- struct{}{}:
				default:
				}
			}()
			verifyCtx, cancel := context.WithTimeout(ctx, r.planTimeout)
			defer cancel()
			r.verifyAndRecord(verifyCtx, plan, provider)
		}()
	default:
		// A later plan pass also verifies an already-completed plan.
		r.queuePlan(plan.ConfigID.Name)
	}
}

// planLatest implements snapshot -> provider replan -> validate -> generation CAS.
func (r *Reconciler) planLatest(ctx context.Context, name string) {
	for retries := 0; retries < 4; retries++ {
		r.mu.RLock()
		managed := r.configs[name]
		if managed == nil {
			r.mu.RUnlock()
			return
		}
		desired := model.CloneDesiredState(managed.Desired)
		dependencies := append([]string(nil), managed.DependsOnConfigs...)
		provider := r.providers[desired.ProviderType]
		r.mu.RUnlock()
		if provider == nil {
			r.setConfigError(name, errors.Errorf("provider %q is not registered", desired.ProviderType))
			return
		}
		if !r.dependenciesMet(&model.ManagedConfig{DependsOnConfigs: dependencies}) {
			return
		}

		snapshot := r.registry.Snapshot(desired.ConfigID)
		expected := model.Generation(0)
		if snapshot.Plan != nil {
			expected = snapshot.Plan.Generation
		}
		observed, err := provider.Inspect(ctx, model.ResourceID{Name: name})
		if err != nil {
			r.setConfigError(name, errors.Wrap(err, "inspect"))
			return
		}
		result, err := provider.Replan(ctx, ReplanRequest{Observed: observed, Desired: desired, Active: snapshot, ProviderDigest: provider.Digest()})
		if err != nil {
			r.setConfigError(name, errors.Wrap(err, "replan"))
			return
		}
		if err := r.registry.ResolveEffects(ctx, desired.ConfigID, result.Resolutions); err != nil {
			r.setConfigError(name, errors.Wrap(err, "resolve effects"))
			return
		}
		if r.registry.IsDeleting(desired.ConfigID) {
			if r.registry.DeletionReady(desired.ConfigID) {
				r.finalizeDeletion(ctx, desired.ConfigID)
			}
			return
		}
		// Resolutions change execution revision/state; re-snapshot before plan CAS.
		if len(result.Resolutions) > 0 {
			snapshot = r.registry.Snapshot(desired.ConfigID)
			if snapshot.Plan != nil {
				expected = snapshot.Plan.Generation
			}
		}
		candidate, err := BuildCandidate(desired.ConfigID, desired, provider.Type(), provider.Digest(), result.Operations)
		if err != nil {
			r.setConfigError(name, errors.Wrap(err, "build candidate"))
			return
		}
		installed, change, err := r.registry.Install(ctx, expected, candidate)
		if errors.Is(err, ErrGenerationChanged) {
			continue
		}
		if err != nil {
			r.setConfigError(name, errors.Wrap(err, "install plan"))
			return
		}
		r.cancelRetired(snapshot, change)
		if len(installed.Nodes) == 0 || planCompleted(installed) {
			r.verifyAndRecord(ctx, installed, provider)
			return
		}
		r.setConfigStatus(name, model.ConfigConverging)
		return
	}
	zap.L().Warn("converge: replan CAS contention", zap.String("config", name))
	r.setConfigError(name, errors.New("replan CAS contention"))
}

func (r *Reconciler) cancelRetired(snapshot model.PlanSnapshot, change PlanChange) {
	byKey := make(map[model.OperationKey]model.AttemptID)
	for _, attempt := range snapshot.Attempts {
		byKey[attempt.NodeKey] = attempt.ID
	}
	for _, key := range change.Cancel {
		attemptID := byKey[key]
		r.mu.RLock()
		cancel := r.cancels[attemptID]
		r.mu.RUnlock()
		if cancel != nil {
			cancel()
		}
	}
}

func (r *Reconciler) executeReady(ctx context.Context) {
	r.mu.RLock()
	ids := make([]model.ConfigID, 0, len(r.configs))
	for _, c := range r.configs {
		ids = append(ids, c.ID)
	}
	r.mu.RUnlock()
	for _, id := range ids {
		plan, operations := r.registry.ReadyOperations(id)
		if plan == nil {
			continue
		}
		for _, operation := range operations {
			// Effect nodes are activated through the EffectControl scheduler
			// pathway: ActivateEffectNode persists the control + transitions the
			// node to WaitingOnControl, consuming no execSem slot and creating
			// no DAG provider Attempt. The EffectControl scheduler owns all
			// EffectProvider RPCs.
			if operation.ExecutionKind == model.ExecutionEffectEnsure ||
				operation.ExecutionKind == model.ExecutionEffectObserve ||
				operation.ExecutionKind == model.ExecutionEffectRelease {
				status, err := r.registry.ActivateEffectNode(ctx, id, plan, operation)
				if err != nil {
					zap.L().Error("converge: activate effect node", zap.String("config", id.Name), zap.String("op", string(operation.Key)), zap.Error(err))
					continue
				}
				// Wake the Run loop so processDueControls drives the newly
				// activated control without blocking in select.
				r.wakeControls()
				if status == model.NodeCompleted {
					// Carry already satisfied; re-scan so downstream nodes that
					// just became ready are dispatched in this pass.
					r.executeReady(ctx)
				}
				continue
			}
			// Capacity is acquired before publishing Running, so the number of
			// durable running attempts and goroutines is bounded together.
			select {
			case r.execSem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			attemptID, err := newAttemptID()
			if err != nil {
				<-r.execSem
				zap.L().Error("converge: generate attempt ID", zap.Error(err))
				continue
			}
			opCtx, cancel := context.WithCancel(ctx)
			if operation.Timeout > 0 {
				opCtx, cancel = context.WithTimeout(ctx, time.Duration(operation.Timeout))
			} else {
				opCtx, cancel = context.WithTimeout(ctx, r.executeTimeout)
			}
			// Register cancellation before exposing Running to close the launch race.
			r.mu.Lock()
			r.cancels[attemptID] = cancel
			r.mu.Unlock()
			attempt, err := r.registry.StartAttempt(ctx, id, plan.Generation, operation.Key, attemptID)
			if err != nil {
				cancel()
				r.mu.Lock()
				delete(r.cancels, attemptID)
				r.mu.Unlock()
				<-r.execSem
				continue
			}
			go r.executeAttempt(ctx, opCtx, cancel, plan, operation, attempt)
		}
	}
}

func newAttemptID() (model.AttemptID, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return model.AttemptID(hex.EncodeToString(value[:])), nil
}

func (r *Reconciler) executeAttempt(ctx, opCtx context.Context, cancel context.CancelFunc, plan *model.Plan, operation model.Operation, attempt *model.Attempt) {
	defer func() { <-r.execSem }()
	r.mu.RLock()
	provider := r.providerVersions[operation.Provider][plan.ProviderDigest]
	r.mu.RUnlock()
	if provider == nil {
		r.publishResult(ctx, plan, operation, attempt, model.StepResult{State: model.StepFailed, Code: "provider_version_unavailable", Reason: "provider implementation matching plan digest is unavailable", Retryable: true})
		return
	}
	defer func() { cancel(); r.mu.Lock(); delete(r.cancels, attempt.ID); r.mu.Unlock() }()

	for _, condition := range operation.Conditions {
		met, err := provider.EvaluateCondition(opCtx, condition)
		if err != nil {
			r.publishResult(ctx, plan, operation, attempt, model.StepResult{State: model.StepFailed, Code: "condition_error", Reason: err.Error(), Retryable: true})
			return
		}
		if !met {
			r.publishResult(ctx, plan, operation, attempt, model.StepResult{State: model.StepWaiting, Code: "condition_unmet", NextCheckAt: time.Now().Add(5 * time.Second)})
			return
		}
	}
	var release func()
	if operation.Destructive && operation.Phase == model.PhaseCommit {
		var err error
		release, err = r.arbiter.Acquire(opCtx, string(attempt.ID))
		if err != nil {
			r.publishResult(ctx, plan, operation, attempt, model.StepResult{State: model.StepFailed, Code: "arbiter_busy", Reason: err.Error()})
			return
		}
		defer release()
	}
	result, err := provider.Execute(opCtx, operation)
	if opCtx.Err() == context.DeadlineExceeded {
		if transitionErr := r.registry.MarkAttemptUnknown(ctx, plan.ConfigID, attempt.ID); transitionErr != nil {
			zap.L().Error("converge: persist timed-out unknown effect", zap.Error(transitionErr))
		}
		// Inspect/Replan is the only safe way to decide whether the effect happened.
		r.queuePlan(plan.ConfigID.Name)
		return
	}
	if err != nil {
		result = model.StepResult{State: model.StepFailed, Code: "execute_error", Reason: err.Error()}
	}
	r.publishResult(ctx, plan, operation, attempt, result)
}

func (r *Reconciler) publishResult(ctx context.Context, plan *model.Plan, operation model.Operation, attempt *model.Attempt, result model.StepResult) {
	event := model.Event{EventID: string(attempt.ID) + "/result", PlanID: plan.ID, Generation: plan.Generation, NodeKey: operation.Key, AttemptID: attempt.ID, ConfigID: plan.ConfigID.Name, State: result.State, Result: result}
	if err := r.registry.EnqueueOutbox(ctx, event); err != nil {
		zap.L().Error("converge: persist event outbox", zap.Error(err))
		return
	}
	r.wakeOutbox()
}

func (r *Reconciler) dispatchOutbox(ctx context.Context) {
	for _, event := range r.registry.PendingOutbox() {
		if err := r.events.Publish(ctx, event); err != nil {
			zap.L().Warn("converge: publish outbox event", zap.Error(err))
			continue
		}
	}
}

func (r *Reconciler) wakeOutbox() {
	select {
	case r.outboxWake <- struct{}{}:
	default:
	}
}

func (r *Reconciler) runOutboxDispatcher(ctx context.Context) {
	retry := time.NewTicker(time.Second)
	defer retry.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.outboxWake:
			r.dispatchOutbox(ctx)
		case <-retry.C:
			r.dispatchOutbox(ctx)
		}
	}
}

func (r *Reconciler) handleEvent(ctx context.Context, event model.Event) {
	if err := r.journal.Append(ctx, event); err != nil {
		zap.L().Error("converge: append journal", zap.Error(err))

		return
	}
	ack := func() {
		if err := r.registry.AckOutbox(ctx, model.ConfigID{Name: event.ConfigID}, event.EventID); err != nil {
			zap.L().Error("converge: acknowledge outbox", zap.Error(err))
		}
	}
	if event.State == model.StepWaiting {
		if err := r.registry.ApplyWaiting(ctx, event); err != nil {
			zap.L().Warn("converge: waiting transition failed; retaining outbox event", zap.Error(err))
			return
		}
		ack()
		return
	}
	if event.State == model.StepFailed && event.Result.Retryable {
		retried, exhausted, err := r.registry.ApplyRetryableFailure(ctx, event)
		if err != nil {
			zap.L().Warn("converge: invalid retry event", zap.Error(err))
			return
		}
		if retried {
			ack()
			r.executeReady(ctx)
			return
		}
		if !exhausted {
			// The attempt was already transitioned by an earlier delivery.
			ack()
			return
		}
	}
	changed, retiredFinished, err := r.registry.ApplyEvent(ctx, event)
	if err != nil {
		zap.L().Warn("converge: event transition failed; retaining outbox event", zap.Error(err))
		return
	}
	ack()
	if retiredFinished {
		configID := model.ConfigID{Name: event.ConfigID}
		if r.registry.IsDeleting(configID) && r.registry.DeletionReady(configID) {
			r.finalizeDeletion(ctx, configID)
		} else {
			r.queuePlan(event.ConfigID)
		}
		return
	}
	if !changed {
		return
	}
	snapshot := r.registry.Snapshot(model.ConfigID{Name: event.ConfigID})
	if snapshot.Plan == nil {
		return
	}
	if planFailed(snapshot.Plan) {
		r.setConfigError(event.ConfigID, errors.Errorf("operation %q failed: %s", event.NodeKey, event.Result.Reason))
		return
	}
	if planCompleted(snapshot.Plan) {
		r.mu.RLock()
		provider := r.providerVersions[snapshot.Plan.ProviderType][snapshot.Plan.ProviderDigest]
		r.mu.RUnlock()
		if provider != nil {
			r.scheduleVerify(ctx, snapshot.Plan, provider)
		}
	}
}

func (r *Reconciler) verifyAndRecord(ctx context.Context, plan *model.Plan, provider Provider) {
	desired := model.CloneDesiredState(plan.Desired)
	observed, err := provider.Verify(ctx, model.ResourceID{Name: plan.ConfigID.Name}, desired)
	if err != nil {
		r.setConfigError(plan.ConfigID.Name, errors.Wrap(err, "verify"))
		return
	}
	// Do not let completion of an old plan record a newer mutable desired state.
	current := r.registry.Snapshot(plan.ConfigID).Plan
	if current == nil || current.ID != plan.ID || current.Generation != plan.Generation ||
		current.DesiredVersion != desired.Version || current.DesiredDigest != desired.Digest {
		return
	}
	recorded := model.RecordedState{ConfigID: plan.ConfigID, ProviderType: provider.Type(), DesiredVersion: desired.Version, DesiredDigest: desired.Digest, HandlerDigest: provider.Digest(), Status: model.ConfigConverged, UpdatedAt: time.Now()}
	if err := r.store.Record(ctx, recorded); err != nil {
		r.setConfigError(plan.ConfigID.Name, errors.Wrap(err, "record converged state"))
		return
	}
	r.mu.Lock()
	managed := r.configs[plan.ConfigID.Name]
	if managed != nil && managed.Desired.Version == desired.Version && managed.Desired.Digest == desired.Digest {
		managed.Observed, managed.Recorded, managed.Status = observed, recorded, model.ConfigConverged
		managed.LastError = ""
	}
	r.mu.Unlock()
	r.wakeDependents(ctx, plan.ConfigID.Name)
}

func (r *Reconciler) dependenciesMet(managed *model.ManagedConfig) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, name := range managed.DependsOnConfigs {
		dependency := r.configs[name]
		if dependency == nil || dependency.Status != model.ConfigConverged {
			return false
		}
	}
	return true
}

func (r *Reconciler) invalidateDependents(_ context.Context, upstream string) {
	for _, name := range r.transitiveDependents(upstream) {
		r.setConfigStatus(name, model.ConfigConverging)
		r.queuePlan(name)
	}
}
func (r *Reconciler) wakeDependents(ctx context.Context, name string) {
	r.invalidateDependents(ctx, name)
}

func (r *Reconciler) deleteConfig(ctx context.Context, name string) {
	r.deleteConfigRequest(ctx, deleteRequest{name: name})
}

func (r *Reconciler) deleteConfigRequest(ctx context.Context, request deleteRequest) {
	name := request.name
	if request.conditional {
		r.mu.RLock()
		managed := r.configs[name]
		matches := managed != nil && managed.Desired.Version == request.version && managed.Desired.Digest == request.digest
		r.mu.RUnlock()
		if !matches {
			return
		}
	}
	// Dependents are deleted first so no converged config can reference a
	// deleted upstream.
	for _, dependent := range r.transitiveDependents(name) {
		r.deleteConfigRequest(ctx, deleteRequest{name: dependent})
	}
	r.mu.RLock()
	managed := r.configs[name]
	r.mu.RUnlock()
	if managed == nil {
		return
	}
	attempts, err := r.registry.MarkDeleting(ctx, managed.ID)
	if err != nil {
		zap.L().Error("converge: mark config deleting", zap.String("config", name), zap.Error(err))
		r.setConfigError(name, errors.Wrap(err, "mark deleting"))
		return
	}
	r.setConfigStatus(name, model.ConfigConverging)
	for _, attempt := range attempts {
		if attempt.Status != model.AttemptCancelling {
			continue
		}
		r.mu.RLock()
		cancel := r.cancels[attempt.ID]
		r.mu.RUnlock()
		if cancel != nil {
			cancel()
		}
	}
	if r.registry.DeletionReady(managed.ID) {
		r.finalizeDeletion(ctx, managed.ID)
	}
}

func (r *Reconciler) finalizeDeletion(ctx context.Context, configID model.ConfigID) {
	// The tombstone remains durable until both user-visible state and execution
	// state are removed. Recorded state is deleted first; a crash after it is
	// harmless because the tombstone resumes deletion on recovery.
	if err := r.store.Delete(ctx, configID); err != nil {
		zap.L().Error("converge: delete recorded state", zap.String("config", configID.Name), zap.Error(err))
		r.setConfigError(configID.Name, errors.Wrap(err, "delete recorded state"))
		return
	}
	if err := r.registry.Delete(ctx, configID); err != nil {
		zap.L().Error("converge: delete execution state", zap.String("config", configID.Name), zap.Error(err))
		r.setConfigError(configID.Name, errors.Wrap(err, "delete execution state"))
		return
	}
	r.mu.Lock()
	delete(r.configs, configID.Name)
	r.mu.Unlock()
}

func (r *Reconciler) detectDrift(ctx context.Context) {
	r.wakeOutbox()
	if err := r.registry.WakeDueWaiting(ctx, time.Now()); err != nil {
		zap.L().Error("converge: wake waiting", zap.Error(err))
	}
	r.mu.RLock()
	var names []string
	for name, c := range r.configs {
		if c.Status == model.ConfigConverged {
			names = append(names, name)
		}
	}
	r.mu.RUnlock()
	for _, name := range names {
		r.queuePlan(name)
	}
}

func (r *Reconciler) setConfigStatus(name string, status model.ConfigStatus) {
	r.mu.Lock()
	if c := r.configs[name]; c != nil {
		c.Status = status
		if status != model.ConfigError {
			c.LastError = ""
		}
	}
	r.mu.Unlock()
}

func (r *Reconciler) setConfigError(name string, err error) {
	r.mu.Lock()
	if c := r.configs[name]; c != nil {
		c.Status = model.ConfigError
		if err != nil {
			c.LastError = err.Error()
		}
	}
	r.mu.Unlock()
}
func planCompleted(plan *model.Plan) bool {
	for _, n := range plan.Nodes {
		if n.Status != model.NodeCompleted {
			return false
		}
	}
	return true
}
func planFailed(plan *model.Plan) bool {
	for _, n := range plan.Nodes {
		if n.Status == model.NodeFailed || n.Status == model.NodeCancelled {
			return true
		}
	}
	return false
}
