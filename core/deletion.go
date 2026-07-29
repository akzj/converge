package core

import (
	"context"
	"time"

	"github.com/akzj/converge/pkg/model"
)

// MarkDeleting durably stops new scheduling and schedules release of effect references.
func (r *PlanRegistry) MarkDeleting(ctx context.Context, configID model.ConfigID) ([]model.Attempt, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.configs[configID.Name]
	if current == nil || current.active == nil {
		return nil, nil
	}
	if current.deleting {
		return deletionAttempts(current), nil
	}
	state := cloneConfigExecution(current)
	state.deleting = true
	for id, attempt := range state.attempts {
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
		if reference.State != EffectReferenceReleaseRequested {
			reference.State = EffectReferenceReleaseRequested
			state.references[id] = reference
		}
		retireIncompatibleControlsLocked(state, reference.ID)
		releaseID := ControlRequestID("release-" + string(reference.ID))
		if _, exists := state.controls[releaseID]; exists {
			continue
		}
		effect := state.effects[reference.EffectID]
		state.controls[releaseID] = EffectControl{
			ID: releaseID, ConfigID: configID,
			ProviderType: effect.ProviderType, ProviderDigest: effect.ProviderDigest,
			Kind: EffectControlRelease, State: EffectControlPending,
			EffectID: effect.ID, ReferenceID: reference.ID, NextCheckAt: time.Now(),
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

// retireIncompatibleControlsLocked completes Observe/Ensure controls that become
// illegal once a reference enters ReleaseRequested.
func retireIncompatibleControlsLocked(state *configExecution, referenceID ReferenceID) {
	for id, control := range state.controls {
		if control.ReferenceID != referenceID || control.State == EffectControlCompleted {
			continue
		}
		switch control.Kind {
		case EffectControlObserve, EffectControlEnsureRetry, EffectControlEnsureReference:
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
	if state == nil || !state.deleting || len(deletionAttempts(state)) > 0 {
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
