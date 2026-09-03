package core

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/akzj/converge/pkg/model"
)

// ResolveEffects applies provider-authoritative assessments of Unknown retired
// attempts. StillActive remains blocked; Completed/Absent release the barrier.
func (r *PlanRegistry) ResolveEffects(ctx context.Context, configID model.ConfigID, resolutions map[model.AttemptID]EffectResolution) error {
	if len(resolutions) == 0 {
		return nil
	}
	unlockConfig := r.lockConfig(configID)
	defer unlockConfig()
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.configs[configID.Name]
	if current == nil || current.active == nil {
		return nil
	}
	state := cloneConfigExecution(current)
	changed := false
	for attemptID, resolution := range resolutions {
		attempt := state.retired[attemptID]
		if attempt == nil || attempt.Status != model.AttemptUnknown {
			return errors.Errorf("resolution references non-unknown attempt %q", attemptID)
		}
		switch resolution {
		case EffectStillActive:
			continue
		case EffectCompleted:
			attempt.Status = model.AttemptCompleted
			delete(state.retired, attemptID)
			if node := state.active.Nodes[attempt.NodeKey]; node != nil && node.AttemptID == attemptID {
				node.Status = model.NodeCompleted
			}
			changed = true
		case EffectAbsent:
			attempt.Status = model.AttemptCancelled
			delete(state.retired, attemptID)
			if node := state.active.Nodes[attempt.NodeKey]; node != nil && node.AttemptID == attemptID {
				node.Status, node.AttemptID = model.NodePending, ""
			}
			changed = true
		default:
			return errors.Errorf("unknown effect resolution %q", resolution)
		}
	}
	if !changed {
		return nil
	}
	if err := r.persistLocked(ctx, configID, state.revision, state); err != nil {
		return err
	}
	r.configs[configID.Name] = state
	return nil
}
