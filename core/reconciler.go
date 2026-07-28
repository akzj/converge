package core

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/akzj/converge/pkg/model"
)

const maxConcurrentExecutions = 10

// Reconciler owns desired state and drives generation-aware plan execution.
type Reconciler struct {
	mu sync.RWMutex

	providers map[string]Provider
	store     StateStore
	events    EventBus
	arbiter   Arbiter
	journal   Journal
	registry  *PlanRegistry

	configs map[string]*model.ManagedConfig
	cancels map[model.AttemptID]context.CancelFunc

	pendingDesired chan model.DesiredState
	pendingDelete  chan string
	execSem        chan struct{}
	attemptSeq     atomic.Uint64
}

func NewReconciler(store StateStore, executionStore ExecutionStore, events EventBus, arbiter Arbiter, journal Journal) *Reconciler {
	return &Reconciler{
		providers: make(map[string]Provider), store: store, events: events, arbiter: arbiter, journal: journal,
		registry: NewPlanRegistry(executionStore), configs: make(map[string]*model.ManagedConfig), cancels: make(map[model.AttemptID]context.CancelFunc),
		pendingDesired: make(chan model.DesiredState, 128), pendingDelete: make(chan string, 128), execSem: make(chan struct{}, maxConcurrentExecutions),
	}
}

func (r *Reconciler) RegisterProvider(ctx context.Context, provider Provider) {
	r.mu.Lock()
	old := r.providers[provider.Type()]
	r.providers[provider.Type()] = provider
	var affected []string
	if old != nil && old.Digest() != provider.Digest() {
		for name, config := range r.configs {
			if config.Desired.ProviderType == provider.Type() {
				affected = append(affected, name)
			}
		}
	}
	r.mu.Unlock()
	for _, name := range affected {
		r.planLatest(ctx, name)
	}
}

func (r *Reconciler) SubmitDesired(ctx context.Context, desired model.DesiredState) error {
	select {
	case r.pendingDesired <- desired:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Reconciler) SubmitDelete(ctx context.Context, name string) error {
	select {
	case r.pendingDelete <- name:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Reconciler) Run(ctx context.Context) error {
	if err := r.recover(ctx); err != nil {
		return errors.Wrap(err, "recover")
	}
	eventCh, err := r.events.Subscribe(ctx, "")
	if err != nil {
		return errors.Wrap(err, "subscribe")
	}
	r.dispatchOutbox(ctx)
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case desired := <-r.pendingDesired:
			r.handleDesired(ctx, desired)
		case name := <-r.pendingDelete:
			r.deleteConfig(ctx, name)
		case event := <-eventCh:
			r.handleEvent(ctx, event)
		case <-ticker.C:
			r.detectDrift(ctx)
		}
		r.executeReady(ctx)
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
	defer r.mu.Unlock()
	for _, id := range ids {
		recorded, err := r.store.Get(ctx, id)
		if err != nil {
			return err
		}
		if recorded == nil {
			continue
		}
		r.configs[id.Name] = &model.ManagedConfig{ID: id, Recorded: *recorded, Status: model.ConfigConverged, Desired: model.DesiredState{ConfigID: id, ProviderType: recorded.ProviderType, Version: recorded.DesiredVersion, Digest: recorded.DesiredDigest}}
	}
	return nil
}

func (r *Reconciler) handleDesired(ctx context.Context, desired model.DesiredState) {
	name := desired.ConfigID.Name
	r.mu.Lock()
	current := r.configs[name]
	if current != nil {
		if desired.Version < current.Desired.Version || (desired.Version == current.Desired.Version && desired.Digest != current.Desired.Digest) {
			r.mu.Unlock()
			zap.L().Error("converge: rejected desired revision conflict", zap.String("config", name))
			return
		}
		if desired.Version == current.Desired.Version && desired.Digest == current.Desired.Digest {
			r.mu.Unlock()
			return
		}
		current.Desired, current.DependsOnConfigs, current.Status = desired, append([]string(nil), desired.DependsOn...), model.ConfigConverging
	} else {
		r.configs[name] = &model.ManagedConfig{ID: desired.ConfigID, Desired: desired, DependsOnConfigs: append([]string(nil), desired.DependsOn...), Status: model.ConfigConverging}
	}
	r.mu.Unlock()
	r.planLatest(ctx, name)
	r.invalidateDependents(ctx, name)
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
		desired := managed.Desired
		provider := r.providers[desired.ProviderType]
		r.mu.RUnlock()
		if provider == nil {
			r.setConfigStatus(name, model.ConfigError)
			return
		}
		if !r.dependenciesMet(managed) {
			return
		}

		snapshot := r.registry.Snapshot(desired.ConfigID)
		expected := model.Generation(0)
		if snapshot.Plan != nil {
			expected = snapshot.Plan.Generation
		}
		observed, err := provider.Inspect(ctx, model.ResourceID{Name: name})
		if err != nil {
			r.setConfigStatus(name, model.ConfigError)
			return
		}
		result, err := provider.Replan(ctx, ReplanRequest{Observed: observed, Desired: desired, Active: snapshot, ProviderDigest: provider.Digest()})
		if err != nil {
			r.setConfigStatus(name, model.ConfigError)
			return
		}
		candidate, err := BuildCandidate(desired.ConfigID, desired, provider.Type(), provider.Digest(), result.Operations)
		if err != nil {
			r.setConfigStatus(name, model.ConfigError)
			return
		}
		installed, change, err := r.registry.Install(ctx, expected, candidate)
		if errors.Is(err, ErrGenerationChanged) {
			continue
		}
		if err != nil {
			r.setConfigStatus(name, model.ConfigError)
			return
		}
		r.cancelRetired(snapshot, change)
		if len(installed.Nodes) == 0 || planCompleted(installed) {
			r.verifyAndRecord(ctx, managed, provider)
			return
		}
		r.setConfigStatus(name, model.ConfigConverging)
		return
	}
	zap.L().Warn("converge: replan CAS contention", zap.String("config", name))
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
			attemptID := model.AttemptID(fmt.Sprintf("%s/%d", plan.ID, r.attemptSeq.Add(1)))
			attempt, err := r.registry.StartAttempt(ctx, id, plan.Generation, operation.Key, attemptID)
			if err != nil {
				continue
			}
			go r.executeAttempt(ctx, plan, operation, attempt)
		}
	}
}

func (r *Reconciler) executeAttempt(ctx context.Context, plan *model.Plan, operation model.Operation, attempt *model.Attempt) {
	r.execSem <- struct{}{}
	defer func() { <-r.execSem }()
	opCtx, cancel := context.WithCancel(ctx)
	if operation.Timeout > 0 {
		opCtx, cancel = context.WithTimeout(ctx, time.Duration(operation.Timeout))
	}
	r.mu.Lock()
	r.cancels[attempt.ID] = cancel
	provider := r.providers[operation.Provider]
	r.mu.Unlock()
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
	r.dispatchOutbox(ctx)
}

func (r *Reconciler) dispatchOutbox(ctx context.Context) {
	for _, event := range r.registry.PendingOutbox() {
		if err := r.events.Publish(ctx, event); err != nil {
			zap.L().Warn("converge: publish outbox event", zap.Error(err))
			return
		}
	}
}

func (r *Reconciler) handleEvent(ctx context.Context, event model.Event) {
	if err := r.journal.Append(ctx, event); err != nil {
		zap.L().Error("converge: append journal", zap.Error(err))
		return
	}
	defer func() {
		if err := r.registry.AckOutbox(ctx, model.ConfigID{Name: event.ConfigID}, event.EventID); err != nil {
			zap.L().Error("converge: acknowledge outbox", zap.Error(err))
		}
	}()
	if event.State == model.StepWaiting {
		if err := r.registry.ApplyWaiting(ctx, event); err != nil {
			zap.L().Warn("converge: invalid waiting event", zap.Error(err))
		}
		return
	}
	if event.State == model.StepFailed && event.Result.Retryable {
		retried, exhausted, err := r.registry.ApplyRetryableFailure(ctx, event)
		if err != nil {
			zap.L().Warn("converge: invalid retry event", zap.Error(err))
			return
		}
		if retried {
			r.executeReady(ctx)
			return
		}
		if !exhausted {
			return
		}
	}
	changed, retiredFinished, err := r.registry.ApplyEvent(ctx, event)
	if err != nil {
		zap.L().Warn("converge: ignored invalid event", zap.Error(err))
		return
	}
	if retiredFinished {
		r.planLatest(ctx, event.ConfigID)
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
		r.setConfigStatus(event.ConfigID, model.ConfigError)
		return
	}
	if planCompleted(snapshot.Plan) {
		r.mu.RLock()
		managed, provider := r.configs[event.ConfigID], r.providers[snapshot.Plan.ProviderType]
		r.mu.RUnlock()
		if managed != nil && provider != nil {
			r.verifyAndRecord(ctx, managed, provider)
		}
	}
}

func (r *Reconciler) verifyAndRecord(ctx context.Context, managed *model.ManagedConfig, provider Provider) {
	observed, err := provider.Verify(ctx, model.ResourceID{Name: managed.ID.Name}, managed.Desired)
	if err != nil {
		r.setConfigStatus(managed.ID.Name, model.ConfigError)
		return
	}
	recorded := model.RecordedState{ConfigID: managed.ID, ProviderType: provider.Type(), DesiredVersion: managed.Desired.Version, DesiredDigest: managed.Desired.Digest, HandlerDigest: provider.Digest(), Status: string(model.ConfigConverged), UpdatedAt: time.Now()}
	if err := r.store.Record(ctx, recorded); err != nil {
		r.setConfigStatus(managed.ID.Name, model.ConfigError)
		return
	}
	r.mu.Lock()
	managed.Observed, managed.Recorded, managed.Status = observed, recorded, model.ConfigConverged
	r.mu.Unlock()
	r.wakeDependents(ctx, managed.ID.Name)
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

func (r *Reconciler) invalidateDependents(ctx context.Context, upstream string) {
	r.mu.RLock()
	var names []string
	for name, c := range r.configs {
		for _, dep := range c.DependsOnConfigs {
			if dep == upstream {
				names = append(names, name)
				break
			}
		}
	}
	r.mu.RUnlock()
	for _, name := range names {
		r.setConfigStatus(name, model.ConfigConverging)
		r.planLatest(ctx, name)
	}
}
func (r *Reconciler) wakeDependents(ctx context.Context, name string) {
	r.invalidateDependents(ctx, name)
}

func (r *Reconciler) deleteConfig(ctx context.Context, name string) {
	r.mu.RLock()
	managed := r.configs[name]
	r.mu.RUnlock()
	if managed == nil {
		return
	}
	// Delete durable execution and final state before publishing the deletion in
	// memory. Any failure leaves the config visible for a safe retry.
	if err := r.registry.Delete(ctx, managed.ID); err != nil {
		zap.L().Error("converge: delete execution state", zap.String("config", name), zap.Error(err))
		return
	}
	if err := r.store.Delete(ctx, managed.ID); err != nil {
		zap.L().Error("converge: delete recorded state", zap.String("config", name), zap.Error(err))
		return
	}
	r.mu.Lock()
	delete(r.configs, name)
	r.mu.Unlock()
}

func (r *Reconciler) detectDrift(ctx context.Context) {
	r.dispatchOutbox(ctx)
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
		r.planLatest(ctx, name)
	}
}

func (r *Reconciler) setConfigStatus(name string, status model.ConfigStatus) {
	r.mu.Lock()
	if c := r.configs[name]; c != nil {
		c.Status = status
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
