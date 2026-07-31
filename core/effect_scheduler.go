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
			ConfigID: reference.ConfigID,
			PlanID:   control.PlanID, Generation: control.Generation,
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
		// Service authoritatively confirmed the job is gone. Use the same
		// single-CAS terminal path as Completed/Cancelled/Failed so the node
		// completion and outbox event are atomic with the effect removal.
		stepState := model.StepCompleted
		event := model.Event{
			EventID: string(identity.AttemptID) + "/control-result",
			PlanID:  identity.EffectIdentity.PlanID, Generation: identity.EffectIdentity.Generation,
			NodeKey: identity.EffectIdentity.OperationKey,
			AttemptID: identity.AttemptID, ConfigID: identity.EffectIdentity.ConfigID.Name,
			State: stepState, Result: model.StepResult{State: stepState},
		}
		if identity.EffectIdentity.OperationKey == "" || identity.EffectIdentity.PlanID == "" {
			_, err := r.registry.ApplyEffectObservation(ctx, identity, *obs.Observation)
			return err
		}
		disposition, err := r.registry.CompleteEffectObservationAndNode(ctx, identity, *obs.Observation, identity.EffectIdentity.OperationKey, event)
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
		stepState := model.StepCompleted
		if obs.Observation.Disposition == DispositionFailed {
			stepState = model.StepFailed
		}
		event := model.Event{
			EventID: string(identity.AttemptID) + "/control-result",
			PlanID:  identity.EffectIdentity.PlanID, Generation: identity.EffectIdentity.Generation,
			NodeKey: nodeKey,
			AttemptID: identity.AttemptID, ConfigID: identity.EffectIdentity.ConfigID.Name,
			State: stepState, Result: model.StepResult{State: stepState},
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
	case EnsureBound:
		// Single CAS: apply the ensure, complete the ensure node, enqueue the
		// lifecycle event atomically. The node is the control's NodeIdentity.
		nodeKey := identity.EffectIdentity.OperationKey
		if nodeKey == "" || identity.EffectIdentity.PlanID == "" {
			// No DAG node to advance; apply the ensure alone.
			_, err := r.registry.ApplyEnsureResult(ctx, identity, result)
			return err
		}
		event := model.Event{
			EventID: string(identity.AttemptID) + "/control-result",
			PlanID:  identity.EffectIdentity.PlanID, Generation: identity.EffectIdentity.Generation,
			NodeKey: nodeKey,
			AttemptID: identity.AttemptID, ConfigID: identity.EffectIdentity.ConfigID.Name,
			State: model.StepCompleted, Result: model.StepResult{State: model.StepCompleted},
		}
		disp, err := r.registry.CompleteEnsureAndNode(ctx, identity, result, nodeKey, event)
		if err != nil {
			return err
		}
		if disp == TransitionApplied || disp == TransitionDuplicate {
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
	case EnsureUnknown:
		_, err := r.registry.ApplyEnsureResult(ctx, identity, result)
		// The control returns to Pending for retry with a fresh AttemptID;
		// retire this claim's attempt as yielded so it does not leak.
		_, _ = r.registry.RetireControlAttempt(ctx, identity, model.AttemptYielded)
		return err
	case EnsureFailed:
		_, err := r.registry.ApplyEnsureResult(ctx, identity, result)
		_, _ = r.registry.RetireControlAttempt(ctx, identity, model.AttemptFailed)
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
	switch result.Disposition {
	case ReleaseStillReferenced, ReleaseConfirmed, ReleaseLastReferenceCancelRequested:
		// Single CAS: apply the release, complete the release node, enqueue the
		// lifecycle event atomically. The node is the control's NodeIdentity.
		nodeKey := identity.EffectIdentity.OperationKey
		if nodeKey == "" || identity.EffectIdentity.PlanID == "" {
			// No DAG node to advance; apply the release alone.
			_, err := r.registry.ApplyReleaseResult(ctx, identity, result)
			return err
		}
		event := model.Event{
			EventID: string(identity.AttemptID) + "/control-result",
			PlanID:  identity.EffectIdentity.PlanID, Generation: identity.EffectIdentity.Generation,
			NodeKey: nodeKey,
			AttemptID: identity.AttemptID, ConfigID: identity.EffectIdentity.ConfigID.Name,
			State: model.StepCompleted, Result: model.StepResult{State: model.StepCompleted},
		}
		disposition, err := r.registry.CompleteReleaseAndNode(ctx, identity, result, nodeKey, event)
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
		_, err := r.registry.ApplyReleaseResult(ctx, identity, result)
		return err
	}
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
	// Single CAS: activate the reference, complete the ensure node, enqueue the
	// lifecycle event atomically. The node is the control's NodeIdentity.
	nodeKey := identity.EffectIdentity.OperationKey
	if nodeKey == "" || identity.EffectIdentity.PlanID == "" {
		// No DAG node to advance; apply the reference activation alone.
		_, err := r.registry.ApplyEnsureReferenceResult(ctx, identity, result)
		return err
	}
	event := model.Event{
		EventID: string(identity.AttemptID) + "/control-result",
		PlanID:  identity.EffectIdentity.PlanID, Generation: identity.EffectIdentity.Generation,
		NodeKey: nodeKey,
		AttemptID: identity.AttemptID, ConfigID: identity.EffectIdentity.ConfigID.Name,
		State: model.StepCompleted, Result: model.StepResult{State: model.StepCompleted},
	}
	disposition, err := r.registry.CompleteEnsureReferenceAndNode(ctx, identity, result, nodeKey, event)
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
}

