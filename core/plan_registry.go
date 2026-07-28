package core

import (
	"fmt"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/akzj/converge/pkg/model"
)

var ErrGenerationChanged = errors.New("active plan generation changed")

type configExecution struct {
	active   *model.Plan
	attempts map[model.AttemptID]*model.Attempt
	retired  map[model.AttemptID]*model.Attempt
}

// PlanRegistry is the concurrency boundary for plans and attempts.
type PlanRegistry struct {
	mu      sync.RWMutex
	configs map[string]*configExecution
}

func NewPlanRegistry() *PlanRegistry {
	return &PlanRegistry{configs: make(map[string]*configExecution)}
}

func (r *PlanRegistry) Snapshot(configID model.ConfigID) model.PlanSnapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	state := r.configs[configID.Name]
	if state == nil {
		return model.PlanSnapshot{}
	}
	result := model.PlanSnapshot{Plan: state.active.Clone()}
	for _, attempt := range state.attempts {
		result.Attempts = append(result.Attempts, *attempt)
	}
	for _, attempt := range state.retired {
		result.Attempts = append(result.Attempts, *attempt)
	}
	return result
}

// Install performs generation CAS and atomically transfers compatible state.
func (r *PlanRegistry) Install(expected model.Generation, candidate *model.Plan) (*model.Plan, PlanChange, error) {
	if candidate == nil {
		return nil, PlanChange{}, errors.New("candidate plan is nil")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.configs[candidate.ConfigID.Name]
	if state == nil {
		state = &configExecution{attempts: make(map[model.AttemptID]*model.Attempt), retired: make(map[model.AttemptID]*model.Attempt)}
		r.configs[candidate.ConfigID.Name] = state
	}
	current := model.Generation(0)
	if state.active != nil {
		current = state.active.Generation
	}
	if current != expected {
		return nil, PlanChange{}, ErrGenerationChanged
	}

	installed := candidate.Clone()
	installed.Generation = current + 1
	installed.ID = model.PlanID(fmt.Sprintf("%s/%d/%s", installed.ConfigID.Name, installed.Generation, installed.DesiredDigest))
	change, err := ClassifyPlanChange(state.active, installed)
	if err != nil {
		return nil, PlanChange{}, err
	}
	if state.active != nil {
		for _, key := range change.Carry {
			oldNode, newNode := state.active.Nodes[key], installed.Nodes[key]
			if oldNode.Status == model.NodeRunning {
				attempt := state.attempts[oldNode.AttemptID]
				if attempt == nil || attempt.Status != model.AttemptRunning || attempt.NodeKey != key || attempt.Fingerprint != oldNode.Operation.Fingerprint {
					return nil, PlanChange{}, errors.Errorf("running operation %q has no matching active attempt", key)
				}
				attempt.PlanID, attempt.Generation, attempt.CarriedTo = installed.ID, installed.Generation, installed.Generation
			}
			newNode.Status, newNode.AttemptID, newNode.RetryCount = oldNode.Status, oldNode.AttemptID, oldNode.RetryCount
		}
		r.retire(state, change.Cancel, model.AttemptCancelling)
		r.retire(state, change.Drain, model.AttemptDraining)
	}
	state.active = installed
	return installed.Clone(), change, nil
}

func (r *PlanRegistry) retire(state *configExecution, keys []model.OperationKey, status model.AttemptStatus) {
	for _, key := range keys {
		node := state.active.Nodes[key]
		if node == nil || node.AttemptID == "" {
			continue
		}
		attempt := state.attempts[node.AttemptID]
		if attempt == nil {
			continue
		}
		attempt.Status = status
		state.retired[attempt.ID] = attempt
		delete(state.attempts, attempt.ID)
	}
}
