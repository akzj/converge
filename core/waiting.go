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
	r.mu.Lock()
	defer r.mu.Unlock()
	for name, current := range r.configs {
		state := cloneConfigExecution(current)
		changed := false
		for id, attempt := range state.attempts {
			if attempt.Status != model.AttemptWaiting || attempt.NextCheckAt.After(now) {
				continue
			}
			node := state.active.Nodes[attempt.NodeKey]
			if node == nil || node.AttemptID != id {
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
				return err
			}
			r.configs[name] = state
		}
	}
	return nil
}
