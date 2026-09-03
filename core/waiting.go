package core

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/akzj/converge/pkg/model"
)

// ApplyWaiting records a non-terminal provider result durably.
func (r *PlanRegistry) ApplyWaiting(ctx context.Context, event model.Event) error {
	if event.AttemptID == "" || event.Result.NextCheckAt.IsZero() {
		return errors.New("waiting event requires attempt ID and next check time")
	}
	unlockConfig := r.lockConfig(model.ConfigID{Name: event.ConfigID})
	defer unlockConfig()
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.configs[event.ConfigID]
	if current == nil || current.active == nil {
		return nil
	}
	state := cloneConfigExecution(current)
	attempt := state.attempts[event.AttemptID]
	if attempt == nil || attempt.NodeKey != event.NodeKey || attempt.PlanID != event.PlanID || attempt.Generation != event.Generation {
		return errors.New("waiting event identity does not match attempt")
	}
	node := state.active.Nodes[event.NodeKey]
	if node == nil || node.AttemptID != event.AttemptID {
		return errors.New("waiting node does not match attempt")
	}
	attempt.Status, attempt.NextCheckAt = model.AttemptWaiting, event.Result.NextCheckAt
	node.Status = model.NodeWaiting
	if err := r.persistLocked(ctx, attempt.ConfigID, state.revision, state); err != nil {
		return err
	}
	r.configs[event.ConfigID] = state
	return nil
}

// WakeDueWaiting moves due nodes back to Pending. Their prior attempt is
// terminally retired so execution will allocate a fresh AttemptID.
func (r *PlanRegistry) WakeDueWaiting(ctx context.Context, now time.Time) error {
	r.mu.RLock()
	ids := make([]model.ConfigID, 0, len(r.configs))
	for name := range r.configs {
		ids = append(ids, model.ConfigID{Name: name})
	}
	r.mu.RUnlock()
	for _, configID := range ids {
		unlockConfig := r.lockConfig(configID)
		r.mu.Lock()
		current := r.configs[configID.Name]
		state := cloneConfigExecution(current)
		changed := false
		if state == nil || state.active == nil {
			r.mu.Unlock()
			unlockConfig()
			continue
		}
		for id, attempt := range state.attempts {
			if attempt.Status != model.AttemptWaiting || attempt.NextCheckAt.After(now) {
				continue
			}
			node := state.active.Nodes[attempt.NodeKey]
			if node == nil || node.AttemptID != id {
				r.mu.Unlock()
				unlockConfig()
				return errors.Errorf("waiting attempt %q does not match active node", id)
			}
			attempt.Status = model.AttemptCompleted
			state.retired[id] = attempt
			delete(state.attempts, id)
			node.Status, node.AttemptID = model.NodePending, ""
			changed = true
		}
		if changed {
			if err := r.persistLocked(ctx, state.active.ConfigID, state.revision, state); err != nil {
				r.mu.Unlock()
				unlockConfig()
				return err
			}
			r.configs[configID.Name] = state
		}
		r.mu.Unlock()
		unlockConfig()
	}
	return nil
}
