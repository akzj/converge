package core

import (
	"context"
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
	store   ExecutionStore
}

var ErrDesiredConflict = errors.New("desired revision conflict")

func NewPlanRegistry(stores ...ExecutionStore) *PlanRegistry {
	var store ExecutionStore
	if len(stores) > 0 {
		store = stores[0]
	}
	return &PlanRegistry{configs: make(map[string]*configExecution), store: store}
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
func (r *PlanRegistry) executionSnapshotLocked(state *configExecution) ExecutionSnapshot {
	if state == nil {
		return ExecutionSnapshot{}
	}
	snapshot := ExecutionSnapshot{Plan: state.active.Clone()}
	for _, attempt := range state.attempts {
		snapshot.Attempts = append(snapshot.Attempts, *attempt)
	}
	for _, attempt := range state.retired {
		snapshot.Attempts = append(snapshot.Attempts, *attempt)
	}
	return snapshot
}

func (r *PlanRegistry) persistLocked(ctx context.Context, id model.ConfigID, expected model.Generation, state *configExecution) error {
	if r.store == nil {
		return nil
	}
	return r.store.CommitExecutionCAS(ctx, id, expected, r.executionSnapshotLocked(state))
}

// Restore loads durable state. Previously-running attempts become Unknown and
// retired, because process loss cannot prove whether external effects stopped.
func (r *PlanRegistry) Restore(ctx context.Context) error {
	if r.store == nil {
		return nil
	}
	ids, err := r.store.ListExecutions(ctx)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, id := range ids {
		snapshot, err := r.store.LoadExecution(ctx, id)
		if err != nil {
			return err
		}
		if snapshot == nil || snapshot.Plan == nil {
			continue
		}
		state := &configExecution{active: snapshot.Plan.Clone(), attempts: make(map[model.AttemptID]*model.Attempt), retired: make(map[model.AttemptID]*model.Attempt)}
		for i := range snapshot.Attempts {
			attempt := snapshot.Attempts[i]
			copy := attempt
			if attempt.Status == model.AttemptRunning || attempt.Status == model.AttemptCancelling || attempt.Status == model.AttemptDraining {
				copy.Status = model.AttemptUnknown
				state.retired[copy.ID] = &copy
				if node := state.active.Nodes[copy.NodeKey]; node != nil && node.AttemptID == copy.ID {
					node.Status = model.NodeDraining
				}
			} else {
				state.attempts[copy.ID] = &copy
			}
		}
		r.configs[id.Name] = state
	}
	return nil
}

func cloneConfigExecution(state *configExecution) *configExecution {
	if state == nil {
		return nil
	}
	copy := &configExecution{active: state.active.Clone(), attempts: make(map[model.AttemptID]*model.Attempt), retired: make(map[model.AttemptID]*model.Attempt)}
	for id, attempt := range state.attempts {
		value := *attempt
		copy.attempts[id] = &value
	}
	for id, attempt := range state.retired {
		value := *attempt
		copy.retired[id] = &value
	}
	return copy
}

// Install performs generation CAS and atomically transfers compatible state.
func (r *PlanRegistry) Install(expected model.Generation, candidate *model.Plan) (*model.Plan, PlanChange, error) {
	if candidate == nil {
		return nil, PlanChange{}, errors.New("candidate plan is nil")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	original := r.configs[candidate.ConfigID.Name]
	state := cloneConfigExecution(original)
	if state == nil {
		state = &configExecution{attempts: make(map[model.AttemptID]*model.Attempt), retired: make(map[model.AttemptID]*model.Attempt)}
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
	if err := r.persistLocked(context.Background(), candidate.ConfigID, current, state); err != nil {
		return nil, PlanChange{}, err
	}
	r.configs[candidate.ConfigID.Name] = state
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
	currentState := r.configs[configID.Name]
	if currentState == nil || currentState.active == nil || currentState.active.Generation != generation {
		return nil, ErrGenerationChanged
	}
	state := cloneConfigExecution(currentState)
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
	if err := r.persistLocked(context.Background(), configID, generation, state); err != nil {
		return nil, err
	}
	r.configs[configID.Name] = state
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
	currentState := r.configs[event.ConfigID]
	if currentState == nil {
		return false, false, nil
	}
	state := cloneConfigExecution(currentState)
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
		if err := r.persistLocked(context.Background(), attempt.ConfigID, state.active.Generation, state); err != nil {
			return false, false, err
		}
		r.configs[event.ConfigID] = state
		return true, false, nil
	}
	delete(state.retired, event.AttemptID)
	if err := r.persistLocked(context.Background(), attempt.ConfigID, state.active.Generation, state); err != nil {
		return false, false, err
	}
	r.configs[event.ConfigID] = state
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

// ReadyOperations returns dependency-ready nodes not blocked by retired effects.
func (r *PlanRegistry) ReadyOperations(configID model.ConfigID) (*model.Plan, []model.Operation) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	state := r.configs[configID.Name]
	if state == nil || state.active == nil {
		return nil, nil
	}
	blocked := make(map[string]bool)
	for _, attempt := range state.retired {
		if attempt.Status == model.AttemptCancelling || attempt.Status == model.AttemptDraining || attempt.Status == model.AttemptUnknown {
			blocked[attempt.ConflictKey] = true
		}
	}
	var ready []model.Operation
	for _, node := range state.active.Nodes {
		if node.Status != model.NodePending || blocked[node.Operation.ConflictKey] {
			continue
		}
		dependenciesDone := true
		for _, dependency := range node.Operation.DependsOn {
			dep := state.active.Nodes[model.OperationKey(dependency)]
			if dep == nil || dep.Status != model.NodeCompleted {
				dependenciesDone = false
				break
			}
		}
		if dependenciesDone {
			ready = append(ready, node.Operation)
		}
	}
	return state.active.Clone(), ready
}
