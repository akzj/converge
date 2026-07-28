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
	revision uint64
	deleting bool
	active   *model.Plan
	attempts map[model.AttemptID]*model.Attempt
	retired  map[model.AttemptID]*model.Attempt
	outbox   map[string]model.Event
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

// ExecutionPlans returns deep copies of all durable active plans.
func (r *PlanRegistry) ExecutionPlans() []*model.Plan {
	r.mu.RLock()
	defer r.mu.RUnlock()
	plans := make([]*model.Plan, 0, len(r.configs))
	for _, state := range r.configs {
		if state.active != nil {
			plans = append(plans, state.active.Clone())
		}
	}
	return plans
}

func (r *PlanRegistry) executionSnapshotLocked(state *configExecution) ExecutionSnapshot {
	if state == nil {
		return ExecutionSnapshot{}
	}
	snapshot := ExecutionSnapshot{Revision: state.revision, Deleting: state.deleting, Plan: state.active.Clone()}
	for _, attempt := range state.attempts {
		snapshot.Attempts = append(snapshot.Attempts, *attempt)
	}
	for _, attempt := range state.retired {
		snapshot.Attempts = append(snapshot.Attempts, *attempt)
	}
	for _, event := range state.outbox {
		snapshot.Outbox = append(snapshot.Outbox, event)
	}
	return snapshot
}

func (r *PlanRegistry) persistLocked(ctx context.Context, id model.ConfigID, expectedRevision uint64, state *configExecution) error {
	state.revision = expectedRevision + 1
	if r.store == nil {
		return nil
	}
	return r.store.CommitExecutionCAS(ctx, id, expectedRevision, r.executionSnapshotLocked(state))
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
		state := &configExecution{revision: snapshot.Revision, deleting: snapshot.Deleting, active: snapshot.Plan.Clone(), attempts: make(map[model.AttemptID]*model.Attempt), retired: make(map[model.AttemptID]*model.Attempt), outbox: make(map[string]model.Event)}
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
		for _, event := range snapshot.Outbox {
			state.outbox[event.EventID] = event
		}
		r.configs[id.Name] = state
	}
	return nil
}

func cloneConfigExecution(state *configExecution) *configExecution {
	if state == nil {
		return nil
	}
	copy := &configExecution{revision: state.revision, deleting: state.deleting, active: state.active.Clone(), attempts: make(map[model.AttemptID]*model.Attempt), retired: make(map[model.AttemptID]*model.Attempt), outbox: make(map[string]model.Event)}
	for id, attempt := range state.attempts {
		value := *attempt
		copy.attempts[id] = &value
	}
	for id, attempt := range state.retired {
		value := *attempt
		copy.retired[id] = &value
	}
	for id, event := range state.outbox {
		copy.outbox[id] = cloneEvent(event)
	}
	return copy
}

func cloneEvent(event model.Event) model.Event {
	copy := event
	copy.Observed.Properties = append([]byte(nil), event.Observed.Properties...)
	return copy
}

// Install performs generation CAS and atomically transfers compatible state.
func (r *PlanRegistry) Install(ctx context.Context, expected model.Generation, candidate *model.Plan) (*model.Plan, PlanChange, error) {
	if candidate == nil {
		return nil, PlanChange{}, errors.New("candidate plan is nil")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	original := r.configs[candidate.ConfigID.Name]
	state := cloneConfigExecution(original)
	if state == nil {
		state = &configExecution{attempts: make(map[model.AttemptID]*model.Attempt), retired: make(map[model.AttemptID]*model.Attempt), outbox: make(map[string]model.Event)}
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
			// Waiting attempts have returned control to Core and can be safely
			// retired when a new plan replaces their node.
			for _, key := range change.Drop {
				oldNode := state.active.Nodes[key]
				if oldNode == nil || oldNode.Status != model.NodeWaiting || oldNode.AttemptID == "" {
					continue
				}
				if attempt := state.attempts[oldNode.AttemptID]; attempt != nil {
					attempt.Status = model.AttemptCancelled
					state.retired[attempt.ID] = attempt
					delete(state.attempts, attempt.ID)
				}
			}
			oldNode, newNode := state.active.Nodes[key], installed.Nodes[key]
			if oldNode.Status == model.NodeRunning {
				attempt := state.attempts[oldNode.AttemptID]
				if attempt == nil || attempt.Status != model.AttemptRunning || attempt.NodeKey != key || attempt.Fingerprint != oldNode.Operation.Fingerprint {
					return nil, PlanChange{}, errors.Errorf("running operation %q has no matching active attempt", key)
				}
				// Preserve source PlanID/Generation so the already-running worker's
				// event identity remains valid; CarriedTo authorizes it to advance
				// the newly installed generation.
				attempt.CarriedTo = installed.Generation
			}
			newNode.Status, newNode.AttemptID, newNode.RetryCount = oldNode.Status, oldNode.AttemptID, oldNode.RetryCount
		}
		r.retire(state, change.Cancel, model.AttemptCancelling)
		r.retire(state, change.Drain, model.AttemptDraining)
	}
	state.active = installed
	if err := r.persistLocked(ctx, candidate.ConfigID, state.revision, state); err != nil {
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
func (r *PlanRegistry) StartAttempt(ctx context.Context, configID model.ConfigID, generation model.Generation, key model.OperationKey, attemptID model.AttemptID) (*model.Attempt, error) {
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
	if _, exists := state.retired[attemptID]; exists {
		return nil, errors.Errorf("attempt %q was already retired", attemptID)
	}
	for _, event := range state.outbox {
		if event.AttemptID == attemptID {
			return nil, errors.Errorf("attempt %q exists in pending outbox", attemptID)
		}
	}
	attempt := &model.Attempt{ID: attemptID, PlanID: state.active.ID, Generation: generation, ConfigID: configID, NodeKey: key, Fingerprint: node.Operation.Fingerprint, ConflictKey: node.Operation.ConflictKey, Status: model.AttemptRunning}
	node.Status, node.AttemptID = model.NodeRunning, attemptID
	state.attempts[attemptID] = attempt
	if err := r.persistLocked(ctx, configID, state.revision, state); err != nil {
		return nil, err
	}
	r.configs[configID.Name] = state
	copy := *attempt
	return &copy, nil
}

// ApplyEvent routes a terminal event strictly by attempt identity.
func (r *PlanRegistry) ApplyEvent(ctx context.Context, event model.Event) (activeChanged, retiredFinished bool, err error) {
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
	if attempt.NodeKey != event.NodeKey || attempt.PlanID != event.PlanID || attempt.Generation != event.Generation {
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
		if (attempt.PlanID != state.active.ID || attempt.Generation != state.active.Generation) && attempt.CarriedTo != state.active.Generation {
			return false, false, errors.New("active attempt generation mismatch")
		}
		node.Status = terminal
		if err := r.persistLocked(ctx, attempt.ConfigID, state.revision, state); err != nil {
			return false, false, err
		}
		r.configs[event.ConfigID] = state
		return true, false, nil
	}
	delete(state.retired, event.AttemptID)
	if err := r.persistLocked(ctx, attempt.ConfigID, state.revision, state); err != nil {
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

// EnqueueOutbox persists an event before any best-effort publication.
func (r *PlanRegistry) EnqueueOutbox(ctx context.Context, event model.Event) error {
	if event.EventID == "" {
		return errors.New("outbox event ID is empty")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.configs[event.ConfigID]
	if current == nil || current.active == nil {
		return errors.New("outbox config has no active plan")
	}
	state := cloneConfigExecution(current)
	state.outbox[event.EventID] = event
	if err := r.persistLocked(ctx, state.active.ConfigID, state.revision, state); err != nil {
		return err
	}
	r.configs[event.ConfigID] = state
	return nil
}

// PendingOutbox returns copies of all durable pending events.
func (r *PlanRegistry) PendingOutbox() []model.Event {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var events []model.Event
	for _, state := range r.configs {
		for _, event := range state.outbox {
			events = append(events, event)
		}
	}
	return events
}

// AckOutbox removes an event after successful processing.
func (r *PlanRegistry) AckOutbox(ctx context.Context, configID model.ConfigID, eventID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.configs[configID.Name]
	if current == nil || current.active == nil {
		return nil
	}
	if _, exists := current.outbox[eventID]; !exists {
		return nil
	}
	state := cloneConfigExecution(current)
	delete(state.outbox, eventID)
	if err := r.persistLocked(ctx, configID, state.revision, state); err != nil {
		return err
	}
	r.configs[configID.Name] = state
	return nil
}

// Delete removes a configuration from memory and durable execution storage.
// Durable deletion happens first so a failed delete cannot expose a partially
// deleted in-memory state that would reappear after restart.
func (r *PlanRegistry) Delete(ctx context.Context, configID model.ConfigID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.store != nil {
		if err := r.store.DeleteExecution(ctx, configID); err != nil {
			return err
		}
	}
	delete(r.configs, configID.Name)
	return nil
}
