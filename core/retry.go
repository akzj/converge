package core

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/akzj/converge/pkg/model"
)

const maxAttemptsPerNode = 4 // initial attempt plus three retries

// ApplyRetryableFailure retires the failed attempt and makes the node pending
// for a fresh AttemptID. exhausted=true means the caller must apply the event as
// a terminal failure instead.
func (r *PlanRegistry) ApplyRetryableFailure(ctx context.Context, event model.Event) (retried, exhausted bool, err error) {
	if event.AttemptID == "" || event.State != model.StepFailed || !event.Result.Retryable {
		return false, false, errors.New("event is not a retryable failure")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.configs[event.ConfigID]
	if current == nil || current.active == nil {
		return false, false, nil
	}
	state := cloneConfigExecution(current)
	attempt := state.attempts[event.AttemptID]
	if attempt == nil {
		// Duplicate/late failure is idempotently ignored.
		return false, false, nil
	}
	node := state.active.Nodes[event.NodeKey]
	if node == nil || node.AttemptID != event.AttemptID || attempt.NodeKey != event.NodeKey ||
		attempt.PlanID != event.PlanID || attempt.Generation != event.Generation {
		return false, false, errors.New("retry event identity does not match active attempt")
	}
	if node.RetryCount+1 >= maxAttemptsPerNode {
		return false, true, nil
	}
	attempt.Status = model.AttemptFailed
	state.retired[attempt.ID] = attempt
	delete(state.attempts, attempt.ID)
	node.RetryCount++
	node.Status, node.AttemptID = model.NodePending, ""
	if err := r.persistLocked(ctx, attempt.ConfigID, state.revision, state); err != nil {
		return false, false, err
	}
	r.configs[event.ConfigID] = state
	return true, false, nil
}
