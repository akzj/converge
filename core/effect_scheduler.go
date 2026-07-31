package core

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/akzj/converge/pkg/model"
)

const effectControlLease = 30 * time.Second

// processDueControls claims due EffectControls and drives EnsureRetry/Observe/Release RPCs.
func (r *Reconciler) processDueControls(ctx context.Context) {
	now := time.Now()
	r.registry.ReclaimExpiredControls(ctx, now)
	refs, _ := r.registry.ListDueControls(ctx, now)
	for _, ref := range refs {
		if err := r.processOneDueControl(ctx, ref, now); err != nil {
			zap.L().Warn("converge: process due control",
				zap.String("config", ref.ConfigID.Name),
				zap.String("control", string(ref.ControlRequestID)),
				zap.Error(err))
		}
	}
}

func (r *Reconciler) processOneDueControl(ctx context.Context, ref DueControlRef, now time.Time) error {
	attemptID, err := newAttemptID()
	if err != nil {
		return err
	}
	pollID := PollRequestID("poll-" + string(attemptID))
	control, err := r.registry.ClaimDueControl(ctx, ref.ConfigID, ref.ControlRequestID, now, attemptID, pollID, now.Add(effectControlLease))
	if err != nil {
		// Not due / already claimed / stale list entry — harmless.
		return nil
	}
	r.mu.RLock()
	provider := r.providerVersions[control.ProviderType][control.ProviderDigest]
	if provider == nil {
		provider = r.providers[control.ProviderType]
	}
	r.mu.RUnlock()
	effectProvider, ok := provider.(EffectProvider)
	if !ok || effectProvider == nil {
		_, _ = r.registry.YieldControl(ctx, controlIdentity(*control, attemptID), now.Add(5*time.Second))
		return nil
	}
	effect, reference, found := r.registry.LookupEffectAndReference(ref.ConfigID, control.EffectID, control.ReferenceID)
	if !found {
		_, _ = r.registry.YieldControl(ctx, controlIdentity(*control, attemptID), now.Add(5*time.Second))
		return nil
	}
	identity := TransitionIdentity{
		EffectIdentity: EffectIdentity{
			EffectID: effect.ID, ReferenceID: reference.ID,
			ConfigID: reference.ConfigID, PlanID: reference.PlanID, Generation: reference.Generation,
			OperationKey: control.OperationKey,
			EffectKey:    reference.EffectKey, ProviderType: effect.ProviderType, ProviderDigest: effect.ProviderDigest,
		},
		AttemptID: attemptID,
		RequestID: control.ID,
	}

	switch control.Kind {
	case EffectControlObserve, EffectControlObserveCancellation:
		return r.runObserveControl(ctx, effectProvider, identity, effect, attemptID, pollID)
	case EffectControlEnsureRetry:
		return r.runEnsureRetryControl(ctx, effectProvider, identity, effect)
	case EffectControlRelease:
		return r.runReleaseControl(ctx, effectProvider, identity, effect)
	case EffectControlEnsureReference:
		return r.runEnsureReferenceControl(ctx, effectProvider, identity, effect)
	default:
		_, _ = r.registry.YieldControl(ctx, identity, now.Add(5*time.Second))
		return nil
	}
}

func controlIdentity(control EffectControl, attemptID model.AttemptID) TransitionIdentity {
	return TransitionIdentity{
		EffectIdentity: EffectIdentity{
			EffectID: control.EffectID, ReferenceID: control.ReferenceID,
			ConfigID: control.ConfigID, ProviderType: control.ProviderType, ProviderDigest: control.ProviderDigest,
		},
		AttemptID: attemptID,
		RequestID: control.ID,
	}
}

func (r *Reconciler) runObserveControl(ctx context.Context, provider EffectProvider, identity TransitionIdentity, effect ActiveEffect, attemptID model.AttemptID, pollID PollRequestID) error {
	observations, err := provider.ObserveEffects(ctx, []ObserveEffectRequest{{
		Identity: identity.EffectIdentity, AttemptID: attemptID, PollRequestID: pollID,
		ExternalJobID: effect.ExternalJobID, ExternalRevision: effect.ExternalRevision,
	}})
	if err != nil {
		_, _ = r.registry.MarkEffectUnknownBound(ctx, identity, time.Now().Add(5*time.Second))
		return err
	}
	obs, ok := observations[pollID]
	if !ok {
		_, _ = r.registry.YieldControl(ctx, identity, time.Now().Add(5*time.Second))
		return nil
	}
	if obs.Error != nil {
		if obs.Error.Retryable {
			_, _ = r.registry.MarkEffectUnknownBound(ctx, identity, time.Now().Add(5*time.Second))
		} else {
			_, _ = r.registry.YieldControl(ctx, identity, time.Now().Add(5*time.Second))
		}
		return nil
	}
	if obs.Observation == nil {
		_, _ = r.registry.YieldControl(ctx, identity, time.Now().Add(5*time.Second))
		return nil
	}
	switch obs.Observation.Disposition {
	case DispositionStillActive:
		next := obs.Observation.NextCheckAt
		if next.IsZero() {
			next = time.Now().Add(5 * time.Second)
		}
		obs.Observation.NextCheckAt = next
		_, err = r.registry.ApplyEffectObservation(ctx, identity, *obs.Observation)
		return err
	case DispositionAbsent:
		// Non-authoritative absence: yield and retry; do not clean up.
		_, err = r.registry.YieldControl(ctx, identity, time.Now().Add(10*time.Second))
		return err
	case DispositionAuthoritativeGone:
		// Service authoritatively confirmed the job is gone.
		disposition, err := r.registry.ApplyEffectObservation(ctx, identity, *obs.Observation)
		if err != nil {
			return err
		}
		if disposition == TransitionApplied || disposition == TransitionDuplicate {
			r.publishControlNodeCompletion(ctx, identity, model.StepCompleted)
		}
		return nil
	case DispositionCompleted, DispositionCancelled, DispositionFailed:
		// Atomically apply the observation, complete the observe node, and
		// enqueue the lifecycle event in one CAS. The node is identified by the
		// control's durable NodeIdentity, not a reverse EffectKey lookup.
		nodeKey := identity.EffectIdentity.OperationKey
		if nodeKey == "" || identity.EffectIdentity.PlanID == "" {
			// No DAG node to advance; apply the observation alone.
			_, err := r.registry.ApplyEffectObservation(ctx, identity, *obs.Observation)
			return err
		}
		event := model.Event{
			EventID: string(identity.AttemptID) + "/control-result",
			PlanID:  identity.EffectIdentity.PlanID, Generation: identity.EffectIdentity.Generation,
			NodeKey: nodeKey,
			AttemptID: identity.AttemptID, ConfigID: identity.EffectIdentity.ConfigID.Name,
			State: model.StepCompleted, Result: model.StepResult{State: model.StepCompleted},
		}
		disposition, err := r.registry.CompleteEffectObservationAndNode(ctx, identity, *obs.Observation, nodeKey, event)
		if err != nil {
			return err
		}
		if disposition == TransitionApplied || disposition == TransitionDuplicate {
			r.wakeOutbox()
			snapshot := r.registry.Snapshot(identity.EffectIdentity.ConfigID)
			if snapshot.Plan != nil && planCompleted(snapshot.Plan) {
				r.mu.RLock()
				provider := r.providerVersions[snapshot.Plan.ProviderType][snapshot.Plan.ProviderDigest]
				r.mu.RUnlock()
				if provider != nil {
					r.verifyAndRecord(ctx, snapshot.Plan, provider)
				}
			}
			r.executeReady(ctx)
		}
		return nil
	default:
		_, _ = r.registry.YieldControl(ctx, identity, time.Now().Add(5*time.Second))
		return nil
	}
}

func (r *Reconciler) runEnsureRetryControl(ctx context.Context, provider EffectProvider, identity TransitionIdentity, effect ActiveEffect) error {
	result, err := provider.EnsureEffect(ctx, EnsureEffectRequest{
		Identity: identity.EffectIdentity, IdempotencyKey: effect.IdempotencyKey,
		ArtifactID: effect.ArtifactID, SemanticFingerprint: effect.SemanticFingerprint,
		EnsureSpec: append([]byte(nil), effect.EnsureSpec...),
	})
	if err != nil {
		_, _ = r.registry.ApplyEnsureResult(ctx, identity, EnsureEffectResult{
			EffectID: effect.ID, ReferenceID: identity.EffectIdentity.ReferenceID,
			Disposition: EnsureUnknown, Failure: EnsureFailureUnknownOutcome,
			Code: "ensure_rpc_error", Reason: err.Error(),
		})
		return err
	}
	switch result.Disposition {
	case EnsureBound, EnsureUnknown, EnsureFailed:
		_, err = r.registry.ApplyEnsureResult(ctx, identity, result)
		return err
	default:
		_, _ = r.registry.YieldControl(ctx, identity, time.Now().Add(5*time.Second))
		return nil
	}
}

func (r *Reconciler) runReleaseControl(ctx context.Context, provider EffectProvider, identity TransitionIdentity, effect ActiveEffect) error {
	result, err := provider.ReleaseEffect(ctx, ReleaseEffectRequest{
		Identity: identity.EffectIdentity, ReleaseRequestID: identity.RequestID,
		ExternalJobID: effect.ExternalJobID, ExternalRevision: effect.ExternalRevision,
	})
	if err != nil {
		_, _ = r.registry.YieldControl(ctx, identity, time.Now().Add(5*time.Second))
		return err
	}
	disposition, err := r.registry.ApplyReleaseResult(ctx, identity, result)
	if err != nil {
		return err
	}
	if disposition == TransitionApplied || disposition == TransitionDuplicate {
		r.publishControlNodeCompletion(ctx, identity, model.StepCompleted)
	}
	return nil
}

func (r *Reconciler) runEnsureReferenceControl(ctx context.Context, provider EffectProvider, identity TransitionIdentity, effect ActiveEffect) error {
	result, err := provider.EnsureReference(ctx, EnsureReferenceRequest{
		Identity: identity.EffectIdentity, RequestID: identity.RequestID,
		ExternalJobID: effect.ExternalJobID,
	})
	if err != nil {
		_, _ = r.registry.YieldControl(ctx, identity, time.Now().Add(5*time.Second))
		return err
	}
	disposition, err := r.registry.ApplyEnsureReferenceResult(ctx, identity, result)
	if err != nil {
		return err
	}
	if disposition == TransitionApplied || disposition == TransitionDuplicate {
		// Same-artifact carry skips DAG EnsureEffect; complete the ensure node
		// (identified by the control's durable NodeIdentity) so observe
		// dependencies can proceed.
		r.publishControlNodeCompletion(ctx, identity, model.StepCompleted)
	}
	return nil
}

func (r *Reconciler) publishControlNodeCompletion(ctx context.Context, identity TransitionIdentity, state model.StepState) {
	// Use the control's durable NodeIdentity (PlanID/Generation/OperationKey)
	// instead of reverse-looking-up the node by EffectKey.
	if identity.EffectIdentity.OperationKey == "" || identity.EffectIdentity.PlanID == "" {
		return
	}
	event := model.Event{
		EventID: string(identity.AttemptID) + "/control-result",
		PlanID:  identity.EffectIdentity.PlanID, Generation: identity.EffectIdentity.Generation,
		NodeKey: identity.EffectIdentity.OperationKey,
		AttemptID: identity.AttemptID, ConfigID: identity.EffectIdentity.ConfigID.Name,
		State: state, Result: model.StepResult{State: state},
	}
	if err := r.registry.CompleteEffectOperation(ctx, identity, identity.EffectIdentity.OperationKey, state); err != nil {
		zap.L().Warn("converge: complete effect operation from control", zap.Error(err))
		return
	}
	if err := r.registry.EnqueueOutbox(ctx, event); err != nil {
		zap.L().Warn("converge: enqueue control completion", zap.Error(err))
		return
	}
	r.wakeOutbox()
	snapshot := r.registry.Snapshot(identity.EffectIdentity.ConfigID)
	if snapshot.Plan != nil && planCompleted(snapshot.Plan) {
		r.mu.RLock()
		provider := r.providerVersions[snapshot.Plan.ProviderType][snapshot.Plan.ProviderDigest]
		r.mu.RUnlock()
		if provider != nil {
			r.verifyAndRecord(ctx, snapshot.Plan, provider)
		}
	}
	r.executeReady(ctx)
}
