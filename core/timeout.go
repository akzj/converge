package core

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/akzj/converge/pkg/model"
)

// MarkAttemptUnknown records that Core stopped waiting but cannot prove the
// external side effect stopped. The conflict barrier remains until provider
// inspection resolves the attempt.
func (r *PlanRegistry) MarkAttemptUnknown(ctx context.Context, configID model.ConfigID, attemptID model.AttemptID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.configs[configID.Name]
	if current == nil || current.active == nil {
		return errors.New("unknown attempt config has no active plan")
	}
	state := cloneConfigExecution(current)
	attempt := state.attempts[attemptID]
	if attempt == nil {
		return errors.Errorf("active attempt %q not found", attemptID)
	}
	node := state.active.Nodes[attempt.NodeKey]
	if node == nil || node.AttemptID != attemptID {
		return errors.New("unknown attempt does not match active node")
	}
	attempt.Status = model.AttemptUnknown
	state.retired[attemptID] = attempt
	delete(state.attempts, attemptID)
	node.Status = model.NodeDraining
	if err := r.persistLocked(ctx, configID, state.revision, state); err != nil {
		return err
	}
	r.configs[configID.Name] = state
	return nil
}
