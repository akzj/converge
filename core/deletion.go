package core

import (
	"context"
	"time"

	"github.com/akzj/converge/pkg/model"
)

// MarkDeleting durably stops new scheduling and schedules release of effect references.
func (r *PlanRegistry) MarkDeleting(ctx context.Context, configID model.ConfigID) ([]model.Attempt, error) {
	unlockConfig := r.lockConfig(configID)
	defer unlockConfig()
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.configs[configID.Name]
	if current == nil {
		return nil, nil
	}
	if current.deleting {
		return deletionAttempts(current), nil
	}
	state := cloneConfigExecution(current)
	state.deleting = true
	for id, attempt := range state.attempts {
		if state.active == nil {
			continue
		}
		node := state.active.Nodes[attempt.NodeKey]
		if node == nil || attempt.Status != model.AttemptRunning {
			continue
		}
		if node.Operation.CancelMode == model.CancelModeNone {
			attempt.Status = model.AttemptDraining
			node.Status = model.NodeDraining
		} else {
			attempt.Status = model.AttemptCancelling
			node.Status = model.NodeCancelling
		}
		state.retired[id] = attempt
		delete(state.attempts, id)
	}
	for id, reference := range state.references {
		if reference.State == EffectReferenceReleased {
			continue
		}
		effect := state.effects[reference.EffectID]
		// Terminal unbound failure: no external job to release.
		if effect.ID != "" && effect.Binding == EffectBindingUnbound && effect.State == ExternalEffectFailed {
			reference.State = EffectReferenceReleased
			state.references[id] = reference
			retireIncompatibleControlsLocked(state, reference.ID)
			continue
		}
		// Ensuring/Unknown Unbound: CancelRequested and retain EnsureRetry for late bind.
		if effect.ID != "" && effect.Binding == EffectBindingUnbound &&
			(effect.State == ExternalEffectEnsuring || effect.State == ExternalEffectUnknown || effect.State == ExternalEffectCancelRequested) {
			if effect.State != ExternalEffectCancelRequested {
				oldEffect := effect
				effect.State = ExternalEffectCancelRequested
				effect.ResolutionRequired = true
				if err := ValidateEffectTransition(oldEffect, effect, EffectTransitionCoreIntent); err != nil {
					return nil, err
				}
				state.effects[effect.ID] = effect
			}
			continue
		}
		if reference.State != EffectReferenceReleaseRequested {
			reference.State = EffectReferenceReleaseRequested
			state.references[id] = reference
		}
		// Once deletion changes an ensuring reference to release-requested,
		// its EnsureReference control is no longer a legal owner of progress.
		// Retire it together with Observe controls before validating the
		// snapshot, then let the maintenance Release control own cleanup.
		retireIncompatibleControlsLocked(state, reference.ID)
		releaseID := ControlRequestID("release-" + string(reference.ID))
		if _, exists := state.controls[releaseID]; exists {
			continue
		}
		state.controls[releaseID] = EffectControl{
			ID: releaseID, ConfigID: configID,
			ProviderType: effect.ProviderType, ProviderDigest: effect.ProviderDigest,
			Kind: EffectControlRelease, State: EffectControlPending,
			TargetKind: EffectTargetMaintenance,
			EffectID:   effect.ID, ReferenceID: reference.ID, NextCheckAt: time.Now(),
		}
	}
	if err := r.persistLocked(ctx, configID, state.revision, state); err != nil {
		return nil, err
	}
	r.configs[configID.Name] = state
	return deletionAttempts(state), nil
}

func deletionAttempts(state *configExecution) []model.Attempt {
	var attempts []model.Attempt
	if state == nil {
		return attempts
	}
	for _, attempt := range state.retired {
		if attempt.Status == model.AttemptCancelling || attempt.Status == model.AttemptDraining || attempt.Status == model.AttemptUnknown {
			attempts = append(attempts, *attempt)
		}
	}
	return attempts
}

// blockingDeletionAttempts excludes effect-operation attempts whose durable
// control has already reached a terminal state. After a crash, an in-flight
// node attempt is conservatively recovered as Unknown, but a later maintenance
// control can still authoritatively release its external reference. Keeping
// that superseded attempt as a deletion barrier would strand the tombstone.
func blockingDeletionAttempts(state *configExecution) []model.Attempt {
	attempts := deletionAttempts(state)
	result := attempts[:0]
	for _, attempt := range attempts {
		// Maintenance controls intentionally have no plan/node identity. Their
		// external uncertainty is represented by the durable control, effect and
		// reference state checked below; a poll Attempt left Unknown by process
		// recovery must not become a second, unresolvable deletion barrier.
		if attempt.PlanID == "" && attempt.Generation == 0 && attempt.NodeKey == "" {
			continue
		}
		resolved := false
		for _, control := range state.controls {
			if control.TargetKind == EffectTargetPlanNode && control.State == EffectControlCompleted &&
				control.PlanID == attempt.PlanID && control.Generation == attempt.Generation &&
				control.OperationKey == attempt.NodeKey {
				resolved = true
				break
			}
		}
		if !resolved {
			result = append(result, attempt)
		}
	}
	return result
}

// retireIncompatibleControlsLocked completes Observe/EnsureReference controls that
// become illegal once a reference enters ReleaseRequested. EnsureRetry is retained
// for CancelRequested Unbound late-ensure repair.
func retireIncompatibleControlsLocked(state *configExecution, referenceID ReferenceID) {
	retireObserveControlsLocked(state, referenceID)
	for id, control := range state.controls {
		if control.ReferenceID != referenceID || control.State == EffectControlCompleted {
			continue
		}
		if control.Kind == EffectControlEnsureReference {
			control.State = EffectControlCompleted
			control.InFlightAttemptID = ""
			control.PollRequestID = ""
			control.LeaseExpiresAt = time.Time{}
			state.controls[id] = control
		}
	}
}

func retireObserveControlsLocked(state *configExecution, referenceID ReferenceID) {
	for id, control := range state.controls {
		if control.ReferenceID != referenceID || control.State == EffectControlCompleted {
			continue
		}
		if control.Kind == EffectControlObserve {
			control.State = EffectControlCompleted
			control.InFlightAttemptID = ""
			control.PollRequestID = ""
			control.LeaseExpiresAt = time.Time{}
			state.controls[id] = control
		}
	}
}

func (r *PlanRegistry) IsDeleting(configID model.ConfigID) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	state := r.configs[configID.Name]
	return state != nil && state.deleting
}

func (r *PlanRegistry) DeletionReady(configID model.ConfigID) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	state := r.configs[configID.Name]
	if state == nil || !state.deleting || len(blockingDeletionAttempts(state)) > 0 {
		return false
	}
	for _, effect := range state.effects {
		if effect.ResolutionRequired {
			return false
		}
	}
	for _, control := range state.controls {
		if control.State != EffectControlCompleted {
			switch control.Kind {
			case EffectControlRelease, EffectControlObserveCancellation, EffectControlEnsureRetry:
				return false
			}
		}
	}
	for _, reference := range state.references {
		if reference.State != EffectReferenceReleased {
			return false
		}
	}
	return true
}
