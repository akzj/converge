package core

import (
	"context"

	"github.com/akzj/converge/pkg/model"
)

// MarkDeleting durably stops new scheduling and classifies active effects.
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
	return state != nil && state.deleting && len(deletionAttempts(state)) == 0
}
