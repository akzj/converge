package core

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/akzj/converge/pkg/model"
)

const maxConcurrentExecutions = 10

// Reconciler owns desired state and drives generation-aware plan execution.
type Reconciler struct {
	mu sync.RWMutex

	providers        map[string]Provider
	providerVersions map[string]map[string]Provider
	store            StateStore
	events           EventBus
	arbiter          Arbiter
	journal          Journal
	registry         *PlanRegistry

	configs map[string]*model.ManagedConfig
	cancels map[model.AttemptID]context.CancelFunc

	pendingDesired chan model.DesiredState
	pendingDelete  chan string
	execSem        chan struct{}
	outboxWake     chan struct{}
}

func NewReconciler(store StateStore, executionStore ExecutionStore, events EventBus, arbiter Arbiter, journal Journal) *Reconciler {
	return &Reconciler{
		providers: make(map[string]Provider), providerVersions: make(map[string]map[string]Provider), store: store, events: events, arbiter: arbiter, journal: journal,
		registry: NewPlanRegistry(executionStore), configs: make(map[string]*model.ManagedConfig), cancels: make(map[model.AttemptID]context.CancelFunc),
		pendingDesired: make(chan model.DesiredState, 128), pendingDelete: make(chan string, 128), execSem: make(chan struct{}, maxConcurrentExecutions), outboxWake: make(chan struct{}, 1),
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
	go r.runOutboxDispatcher(ctx)
	r.wakeOutbox()
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
		r.processDueControls(ctx)
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
	// Execution plans are the authority for in-progress/newer desired revisions.
	for _, plan := range r.registry.ExecutionPlans() {
		desired := model.CloneDesiredState(plan.Desired)
		if desired.ConfigID.Name == "" {
			desired.ConfigID = plan.ConfigID
		}
		managed := r.configs[plan.ConfigID.Name]
		if managed == nil {
			managed = &model.ManagedConfig{ID: plan.ConfigID}
			r.configs[plan.ConfigID.Name] = managed
		}
		if desired.Version >= managed.Desired.Version {
			managed.Desired = desired
			managed.DependsOnConfigs = append([]string(nil), desired.DependsOn...)
			managed.Status = model.ConfigConverging
		}
	}
	r.mu.Unlock()
	// Unknown/draining effects need active inspection/replanning after recovery.
	for _, plan := range r.registry.ExecutionPlans() {
		if r.registry.IsDeleting(plan.ConfigID) && r.registry.DeletionReady(plan.ConfigID) {
			r.finalizeDeletion(ctx, plan.ConfigID)
			continue
		}
		r.mu.RLock()
		providerAvailable := r.providers[plan.ProviderType] != nil
		r.mu.RUnlock()
		if providerAvailable {
			r.planLatest(ctx, plan.ConfigID.Name)
		}
	}
	return nil
}

func (r *Reconciler) handleDesired(ctx context.Context, desired model.DesiredState) {
	name := desired.ConfigID.Name
	if err := r.validateConfigDependencies(desired); err != nil {
		zap.L().Error("converge: rejected configuration dependency graph", zap.String("config", name), zap.Error(err))
		return
	}
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
		if err := r.registry.ResolveEffects(ctx, desired.ConfigID, result.Resolutions); err != nil {
			r.setConfigStatus(name, model.ConfigError)
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
			r.verifyAndRecord(ctx, installed, provider)
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
			if operation.ExecutionKind == model.ExecutionEffectEnsure ||
				operation.ExecutionKind == model.ExecutionEffectObserve ||
				operation.ExecutionKind == model.ExecutionEffectRelease {
				go r.executeEffectAttempt(ctx, plan, operation, attempt)
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
		r.planLatest(ctx, plan.ConfigID.Name)
		return
	}
	if err != nil {
		result = model.StepResult{State: model.StepFailed, Code: "execute_error", Reason: err.Error()}
	}
	r.publishResult(ctx, plan, operation, attempt, result)
}

func (r *Reconciler) executeEffectAttempt(ctx context.Context, plan *model.Plan, operation model.Operation, attempt *model.Attempt) {
	defer func() { <-r.execSem }()
	r.mu.RLock()
	provider := r.providerVersions[operation.Provider][plan.ProviderDigest]
	r.mu.RUnlock()
	if provider == nil {
		r.publishResult(ctx, plan, operation, attempt, model.StepResult{State: model.StepFailed, Code: "effect_provider_unavailable", Reason: "provider implementation matching plan digest is unavailable", Retryable: true})
		return
	}
	effectProvider, ok := provider.(EffectProvider)
	if !ok {
		r.publishResult(ctx, plan, operation, attempt, model.StepResult{State: model.StepFailed, Code: "effect_provider_unsupported", Reason: "provider does not implement EffectProvider"})
		return
	}

	switch operation.ExecutionKind {
	case model.ExecutionEffectEnsure:
		r.executeEffectEnsure(ctx, plan, operation, attempt, effectProvider)
	case model.ExecutionEffectObserve:
		r.executeEffectObserve(ctx, plan, operation, attempt, effectProvider)
	case model.ExecutionEffectRelease:
		r.executeEffectRelease(ctx, plan, operation, attempt, effectProvider)
	default:
		r.publishResult(ctx, plan, operation, attempt, model.StepResult{State: model.StepFailed, Code: "unknown_effect_kind", Reason: string(operation.ExecutionKind)})
	}
}

func (r *Reconciler) effectIdentity(plan *model.Plan, operation model.Operation, effectID EffectID) EffectIdentity {
	return EffectIdentity{
		EffectID:       effectID,
		ReferenceID:    newReferenceID(plan.ConfigID, plan.ID, plan.Generation, operation.EffectKey),
		ConfigID:       plan.ConfigID,
		PlanID:         plan.ID,
		Generation:     plan.Generation,
		OperationKey:   operation.Key,
		EffectKey:      operation.EffectKey,
		ProviderType:   plan.ProviderType,
		ProviderDigest: plan.ProviderDigest,
	}
}

func (r *Reconciler) executeEffectEnsure(ctx context.Context, plan *model.Plan, operation model.Operation, attempt *model.Attempt, effectProvider EffectProvider) {
	effect, ref, found := r.registry.LookupEffectBinding(plan.ConfigID, plan.ID, plan.Generation, operation.EffectKey)
	if found && effect.Binding == EffectBindingBound && effect.ExternalJobID != "" {
		// Same-artifact carry: EnsureReference owns the new reference; never
		// CreateOrGet a second job under a new idempotency key. The ensure node
		// completes when the EnsureReference control activates the reference.
		if ref.State == EffectReferenceActive {
			r.publishResult(ctx, plan, operation, attempt, model.StepResult{State: model.StepCompleted})
			return
		}
		r.publishResult(ctx, plan, operation, attempt, model.StepResult{
			State: model.StepWaiting, Code: "ensure_reference_pending", NextCheckAt: time.Now().Add(time.Second),
		})
		return
	}
	var effectID EffectID
	if found {
		effectID = effect.ID
	} else {
		generated, err := newEffectID()
		if err != nil {
			r.publishResult(ctx, plan, operation, attempt, model.StepResult{State: model.StepFailed, Code: "effect_id_error", Reason: err.Error(), Retryable: true})
			return
		}
		effectID = generated
	}
	identity := TransitionIdentity{
		EffectIdentity: r.effectIdentity(plan, operation, effectID),
		AttemptID:      attempt.ID,
		RequestID:      ControlRequestID("ensure-" + string(effectID)),
	}
	spec := ImmutableEnsureSpec{
		IdempotencyKey:      "ensure-" + string(plan.ID) + "-" + operation.EffectKey,
		ArtifactID:          plan.Desired.Digest,
		SemanticFingerprint: operation.Fingerprint,
		EnsureSpec:          append([]byte(nil), plan.Desired.Spec...),
	}
	if !found {
		if disposition, err := r.registry.BeginEnsureEffect(ctx, BeginEnsureRequest{Identity: identity, Spec: spec}); err != nil {
			r.publishResult(ctx, plan, operation, attempt, model.StepResult{State: model.StepFailed, Code: "begin_ensure_failed", Reason: err.Error(), Retryable: true})
			return
		} else if disposition != TransitionApplied && disposition != TransitionDuplicate {
			r.publishResult(ctx, plan, operation, attempt, model.StepResult{State: model.StepFailed, Code: "begin_ensure_rejected", Reason: string(disposition), Retryable: true})
			return
		}
	}
	// The EffectControl scheduler is the sole owner of EnsureEffect RPCs. This
	// DAG path only persists the durable ensure intent and yields; the
	// scheduler's EnsureRetry control drives the RPC and completes the node.
	if disp, err := r.registry.EnsureEnsureRetryControl(ctx, plan.ConfigID, effectID, identity.EffectIdentity.ReferenceID, plan.ID, plan.Generation, operation.Key); err != nil || disp != TransitionApplied && disp != TransitionDuplicate {
		r.publishResult(ctx, plan, operation, attempt, model.StepResult{
			State: model.StepWaiting, Code: "ensure_control_pending", NextCheckAt: time.Now().Add(time.Second),
		})
		return
	}
	r.publishResult(ctx, plan, operation, attempt, model.StepResult{
		State: model.StepWaiting, Code: "ensure_scheduled", NextCheckAt: time.Now().Add(time.Second),
	})
}

func (r *Reconciler) executeEffectObserve(ctx context.Context, plan *model.Plan, operation model.Operation, attempt *model.Attempt, effectProvider EffectProvider) {
	effect, _, found := r.registry.LookupEffectBinding(plan.ConfigID, plan.ID, plan.Generation, operation.EffectKey)
	if !found || effect.ExternalJobID == "" {
		r.publishResult(ctx, plan, operation, attempt, model.StepResult{State: model.StepFailed, Code: "observe_unbound", Reason: "no bound effect for observe", Retryable: true})
		return
	}
	// The EffectControl scheduler is the sole owner of ObserveEffects RPCs.
	// This DAG path only verifies that an Observe control exists and yields so
	// the scheduler can drive the poll. If the control is missing, enqueue a
	// control so the scheduler picks it up on the next due sweep.
	identity := TransitionIdentity{
		EffectIdentity: r.effectIdentity(plan, operation, effect.ID),
		AttemptID:      attempt.ID,
		RequestID:      ControlRequestID("observe-" + string(effect.ID)),
	}
	if !r.registry.HasActiveEffectControl(plan.ConfigID, operation.EffectKey, EffectControlObserve) {
		// Ensure an Observe control exists so the scheduler can drive polling.
		if disp, err := r.registry.EnsureObserveControl(ctx, plan.ConfigID, effect.ID, identity.EffectIdentity.ReferenceID, plan.ID, plan.Generation, operation.Key); err != nil || disp != TransitionApplied {
			r.publishResult(ctx, plan, operation, attempt, model.StepResult{
				State: model.StepWaiting, Code: "observe_control_pending",
				NextCheckAt: time.Now().Add(time.Second),
			})
			return
		}
	}
	r.publishResult(ctx, plan, operation, attempt, model.StepResult{
		State:       model.StepWaiting,
		Code:        "in_progress",
		NextCheckAt: time.Now().Add(time.Second),
	})
}

func (r *Reconciler) executeEffectRelease(ctx context.Context, plan *model.Plan, operation model.Operation, attempt *model.Attempt, effectProvider EffectProvider) {
	effect, ref, found := r.registry.LookupEffectBinding(plan.ConfigID, plan.ID, plan.Generation, operation.EffectKey)
	if operation.ReleaseTarget == model.ReleaseRetiredReference && operation.TargetReference != "" {
		ref = EffectReference{ID: ReferenceID(operation.TargetReference), EffectKey: operation.EffectKey}
		if lookedUp, ok := r.registry.LookupReference(plan.ConfigID, ReferenceID(operation.TargetReference)); ok {
			ref = lookedUp
			if eff, ok := r.registry.LookupEffect(plan.ConfigID, ref.EffectID); ok {
				effect = eff
				found = true
			}
		}
	}
	if !found {
		r.publishResult(ctx, plan, operation, attempt, model.StepResult{State: model.StepFailed, Code: "release_missing", Reason: "no effect binding for release", Retryable: true})
		return
	}
	identity := TransitionIdentity{
		EffectIdentity: r.effectIdentity(plan, operation, effect.ID),
		AttemptID:      attempt.ID,
		RequestID:      ControlRequestID("release-" + string(ref.ID)),
	}
	identity.EffectIdentity.ReferenceID = ref.ID
	if disposition, err := r.registry.BeginReleaseEffect(ctx, BeginReleaseRequest{Identity: identity}); err != nil {
		r.publishResult(ctx, plan, operation, attempt, model.StepResult{State: model.StepFailed, Code: "begin_release_failed", Reason: err.Error(), Retryable: true})
		return
	} else if disposition != TransitionApplied && disposition != TransitionDuplicate {
		r.publishResult(ctx, plan, operation, attempt, model.StepResult{State: model.StepFailed, Code: "begin_release_rejected", Reason: string(disposition), Retryable: true})
		return
	}
	// The EffectControl scheduler is the sole owner of ReleaseEffect RPCs.
	// This DAG path marks the release intent (via BeginReleaseEffect) and
	// yields so the scheduler can drive the release.
	r.publishResult(ctx, plan, operation, attempt, model.StepResult{
		State: model.StepWaiting, Code: "release_scheduled", NextCheckAt: time.Now().Add(time.Second),
	})
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
			r.planLatest(ctx, event.ConfigID)
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
		r.setConfigStatus(event.ConfigID, model.ConfigError)
		return
	}
	if planCompleted(snapshot.Plan) {
		r.mu.RLock()
		provider := r.providerVersions[snapshot.Plan.ProviderType][snapshot.Plan.ProviderDigest]
		r.mu.RUnlock()
		if provider != nil {
			r.verifyAndRecord(ctx, snapshot.Plan, provider)
		}
	}
}

func (r *Reconciler) verifyAndRecord(ctx context.Context, plan *model.Plan, provider Provider) {
	desired := model.CloneDesiredState(plan.Desired)
	observed, err := provider.Verify(ctx, model.ResourceID{Name: plan.ConfigID.Name}, desired)
	if err != nil {
		r.setConfigStatus(plan.ConfigID.Name, model.ConfigError)
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
		r.setConfigStatus(plan.ConfigID.Name, model.ConfigError)
		return
	}
	r.mu.Lock()
	managed := r.configs[plan.ConfigID.Name]
	if managed != nil && managed.Desired.Version == desired.Version && managed.Desired.Digest == desired.Digest {
		managed.Observed, managed.Recorded, managed.Status = observed, recorded, model.ConfigConverged
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

func (r *Reconciler) invalidateDependents(ctx context.Context, upstream string) {
	for _, name := range r.transitiveDependents(upstream) {
		r.setConfigStatus(name, model.ConfigConverging)
		r.planLatest(ctx, name)
	}
}
func (r *Reconciler) wakeDependents(ctx context.Context, name string) {
	r.invalidateDependents(ctx, name)
}

func (r *Reconciler) deleteConfig(ctx context.Context, name string) {
	// Dependents are deleted first so no converged config can reference a
	// deleted upstream.
	for _, dependent := range r.transitiveDependents(name) {
		r.deleteConfig(ctx, dependent)
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
		return
	}
	if err := r.registry.Delete(ctx, configID); err != nil {
		zap.L().Error("converge: delete execution state", zap.String("config", configID.Name), zap.Error(err))
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
