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

var ErrDesiredConflict = errors.New("desired revision conflict")

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
	if state.active != nil {
		if candidate.DesiredVersion < state.active.DesiredVersion ||
			(candidate.DesiredVersion == state.active.DesiredVersion && candidate.DesiredDigest != state.active.DesiredDigest) {
			return nil, PlanChange{}, ErrDesiredConflict
		}
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

// StartAttempt atomically transitions one pending/ready node to Running.
func (r *PlanRegistry) StartAttempt(configID model.ConfigID, generation model.Generation, key model.OperationKey, attemptID model.AttemptID) (*model.Attempt, error) {
	if attemptID == "" {
		return nil, errors.New("attempt ID is empty")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.configs[configID.Name]
	if state == nil || state.active == nil || state.active.Generation != generation {
		return nil, ErrGenerationChanged
	}
	node := state.active.Nodes[key]
	if node == nil {
		return nil, errors.Errorf("operation %q not found", key)
	}
	if node.Status != model.NodePending && node.Status != model.NodeReady {
		return nil, errors.Errorf("operation %q is not startable: %s", key, node.Status)
	}
	if _, exists := state.attempts[attemptID]; exists {
		return nil, errors.Errorf("attempt %q already exists", attemptID)
	}
	attempt := &model.Attempt{ID: attemptID, PlanID: state.active.ID, Generation: generation, ConfigID: configID, NodeKey: key, Fingerprint: node.Operation.Fingerprint, ConflictKey: node.Operation.ConflictKey, Status: model.AttemptRunning}
	node.Status, node.AttemptID = model.NodeRunning, attemptID
	state.attempts[attemptID] = attempt
	copy := *attempt
	return &copy, nil
}

// ApplyEvent routes a terminal event strictly by attempt identity.
func (r *PlanRegistry) ApplyEvent(event model.Event) (activeChanged, retiredFinished bool, err error) {
	if event.AttemptID == "" {
		return false, false, errors.New("event attempt ID is empty")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.configs[event.ConfigID]
	if state == nil {
		return false, false, nil
	}
	attempt, active := state.attempts[event.AttemptID]
	if !active {
		attempt = state.retired[event.AttemptID]
	}
	if attempt == nil {
		return false, false, nil
	}
	if attempt.NodeKey != event.NodeKey {
		return false, false, errors.New("event identity does not match attempt")
	}
	next, terminal, err := terminalAttemptStatus(event.State)
	if err != nil {
		return false, false, err
	}
	if isTerminalAttempt(attempt.Status) {
		return false, false, nil
	}
	attempt.Status = next
	if active {
		node := state.active.Nodes[event.NodeKey]
		if node == nil || node.AttemptID != event.AttemptID {
			return false, false, errors.New("active node does not match attempt")
		}
		if attempt.PlanID != state.active.ID || attempt.Generation != state.active.Generation {
			return false, false, errors.New("active attempt generation mismatch")
		}
		node.Status = terminal
		return true, false, nil
	}
	delete(state.retired, event.AttemptID)
	return false, true, nil
}

func terminalAttemptStatus(state model.StepState) (model.AttemptStatus, model.NodeStatus, error) {
	switch state {
	case model.StepCompleted:
		return model.AttemptCompleted, model.NodeCompleted, nil
	case model.StepFailed:
		return model.AttemptFailed, model.NodeFailed, nil
	case model.StepCancelled:
		return model.AttemptCancelled, model.NodeCancelled, nil
	default:
		return "", "", errors.Errorf("event state %q is not terminal", state)
	}
}

func isTerminalAttempt(status model.AttemptStatus) bool {
	return status == model.AttemptCompleted || status == model.AttemptFailed || status == model.AttemptCancelled
}
