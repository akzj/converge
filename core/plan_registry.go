package core

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/akzj/converge/observability"
	"github.com/akzj/converge/pkg/model"
)

var ErrGenerationChanged = errors.New("active plan generation changed")

type configExecution struct {
	revision   uint64
	deleting   bool
	accepted   *model.DesiredState
	active     *model.Plan
	attempts   map[model.AttemptID]*model.Attempt
	retired    map[model.AttemptID]*model.Attempt
	outbox     map[string]model.Event
	effects    map[EffectID]ActiveEffect
	references map[ReferenceID]EffectReference
	controls   map[ControlRequestID]EffectControl
}

// PlanRegistry is the concurrency boundary for plans and attempts.
type PlanRegistry struct {
	mu       sync.RWMutex
	configs  map[string]*configExecution
	store    ExecutionStore
	locks    sync.Map // map[config name]*sync.Mutex
	observer observability.Observer
}

var ErrDesiredConflict = errors.New("desired revision conflict")

func NewPlanRegistry(stores ...ExecutionStore) *PlanRegistry {
	var store ExecutionStore
	if len(stores) > 0 {
		store = stores[0]
	}
	return &PlanRegistry{configs: make(map[string]*configExecution), store: store, observer: observability.Noop()}
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

// Execution returns a detached durable-state view for status and diagnostics.
// Provider payloads remain inside Desired/Operation and should be redacted by
// any external API built on this method.
func (r *PlanRegistry) Execution(configID model.ConfigID) ExecutionSnapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.executionSnapshotLocked(r.configs[configID.Name])
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

// AcceptedDesireds returns the durable desired revisions, including revisions
// that have not produced an executable plan yet.
func (r *PlanRegistry) AcceptedDesireds() []model.DesiredState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]model.DesiredState, 0, len(r.configs))
	for _, state := range r.configs {
		if state.accepted != nil {
			result = append(result, model.CloneDesiredState(*state.accepted))
		}
	}
	return result
}

func (r *PlanRegistry) AcceptedDesired(configID model.ConfigID) (model.DesiredState, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	state := r.configs[configID.Name]
	if state == nil || state.accepted == nil {
		return model.DesiredState{}, false
	}
	return model.CloneDesiredState(*state.accepted), true
}

func (r *PlanRegistry) executionSnapshotLocked(state *configExecution) ExecutionSnapshot {
	if state == nil {
		return ExecutionSnapshot{}
	}
	snapshot := ExecutionSnapshot{Revision: state.revision, Deleting: state.deleting, Plan: state.active.Clone()}
	if state.accepted != nil {
		desired := model.CloneDesiredState(*state.accepted)
		snapshot.AcceptedDesired = &desired
	}
	for _, attempt := range state.attempts {
		snapshot.Attempts = append(snapshot.Attempts, *attempt)
	}
	for _, attempt := range state.retired {
		snapshot.Attempts = append(snapshot.Attempts, *attempt)
	}
	for _, event := range state.outbox {
		snapshot.Outbox = append(snapshot.Outbox, event)
	}
	for _, effect := range state.effects {
		snapshot.Effects = append(snapshot.Effects, effect.Clone())
	}
	for _, reference := range state.references {
		snapshot.EffectReferences = append(snapshot.EffectReferences, reference)
	}
	for _, control := range state.controls {
		snapshot.EffectControls = append(snapshot.EffectControls, control)
	}
	return snapshot
}

func (r *PlanRegistry) persistLocked(ctx context.Context, id model.ConfigID, expectedRevision uint64, state *configExecution) error {
	previous := r.configs[id.Name]
	fillMissingCausalContext(state)
	originalRevision := state.revision
	state.revision = expectedRevision + 1
	snapshot := r.executionSnapshotLocked(state)
	if err := ValidateEffectSnapshot(snapshot); err != nil {
		state.revision = originalRevision
		return errors.Wrap(err, "validate execution snapshot before persist")
	}
	if r.store == nil {
		r.emitCommittedDiff(ctx, id, previous, state)
		return nil
	}
	// The caller holds the config-specific mutex, so the global map lock can be
	// released during storage I/O without allowing a same-config writer to pass.
	r.mu.Unlock()
	err := r.store.CommitExecutionCAS(ctx, id, expectedRevision, snapshot)
	r.mu.Lock()
	if err != nil {
		state.revision = originalRevision
		return err
	}
	r.emitCommittedDiff(ctx, id, previous, state)
	return nil
}

// emitCommittedDiff runs only after the execution snapshot has been committed.
// It deliberately derives diagnostic transitions from durable state and never
// from an attempted mutation, so a failed CAS cannot produce a success signal.
func (r *PlanRegistry) emitCommittedDiff(ctx context.Context, id model.ConfigID, previous, next *configExecution) {
	if next == nil {
		return
	}
	emit := func(transition observability.Transition) {
		transition.ExecutionRevision = next.revision
		transition.At = time.Now()
		transition.ConfigID = id
		r.observer.Committed(ctx, transition)
	}
	if next.accepted != nil && (previous == nil || previous.accepted == nil ||
		previous.accepted.Version != next.accepted.Version || previous.accepted.Digest != next.accepted.Digest) {
		emit(observability.Transition{
			ID:   fmt.Sprintf("config/%s/revision/%d/desired-accepted", id.Name, next.revision),
			Kind: observability.TransitionDesiredAccepted, Provider: next.accepted.ProviderType,
			To: "accepted", Outcome: "accepted", Cause: next.accepted.Cause,
		})
	}
	if next.active != nil && (previous == nil || previous.active == nil || previous.active.ID != next.active.ID) {
		emit(observability.Transition{
			ID:   fmt.Sprintf("config/%s/revision/%d/plan-installed", id.Name, next.revision),
			Kind: observability.TransitionPlanInstalled, PlanID: next.active.ID,
			Generation: next.active.Generation, Provider: next.active.ProviderType,
			To: "installed", Outcome: "success", Cause: next.active.Desired.Cause,
		})
	}
	previousAttempts := allAttempts(previous)
	nextAttempts := allAttempts(next)
	for attemptID, attempt := range nextAttempts {
		old := previousAttempts[attemptID]
		provider, phase := attemptContext(next, previous, attempt)
		outcome, code, reason := attemptResult(next, attemptID, attempt.Status)
		switch {
		case old == nil && attempt.Status == model.AttemptRunning:
			emit(observability.Transition{
				ID: "attempt/" + string(attemptID) + "/running", Kind: observability.TransitionAttemptStarted,
				PlanID: attempt.PlanID, Generation: attempt.Generation, Operation: attempt.NodeKey,
				AttemptID: attempt.ID, Provider: provider, Phase: phase,
				To: string(attempt.Status), Outcome: "success", Cause: attempt.Cause,
			})
		case old != nil && old.Status != attempt.Status:
			emit(observability.Transition{
				ID: "attempt/" + string(attemptID) + "/" + string(attempt.Status), Kind: observability.TransitionAttemptFinished,
				PlanID: attempt.PlanID, Generation: attempt.Generation, Operation: attempt.NodeKey,
				AttemptID: attempt.ID, Provider: provider, Phase: phase,
				From: string(old.Status), To: string(attempt.Status), Outcome: outcome, Code: code, Reason: reason, Cause: attempt.Cause,
			})
		case old != nil && old.CarriedTo != attempt.CarriedTo && attempt.CarriedTo != 0:
			emit(observability.Transition{
				ID: fmt.Sprintf("attempt/%s/carried/%d", attemptID, attempt.CarriedTo), Kind: observability.TransitionAttemptCarried,
				PlanID: next.active.ID, Generation: attempt.CarriedTo, Operation: attempt.NodeKey,
				AttemptID: attempt.ID, Provider: provider, Phase: phase,
				From: fmt.Sprint(old.Generation), To: fmt.Sprint(attempt.CarriedTo), Outcome: "carried", Cause: attempt.Cause,
			})
		}
	}
	for attemptID, old := range previousAttempts {
		if nextAttempts[attemptID] != nil {
			continue
		}
		event, ok := outboxEventForAttempt(next, attemptID)
		if !ok {
			continue
		}
		status, _, err := terminalAttemptStatus(event.State)
		if err != nil {
			continue
		}
		provider, phase := attemptContext(next, previous, old)
		emit(observability.Transition{
			ID: "attempt/" + string(attemptID) + "/" + string(status), Kind: observability.TransitionAttemptFinished,
			PlanID: old.PlanID, Generation: old.Generation, Operation: old.NodeKey, AttemptID: old.ID,
			Provider: provider, Phase: phase, From: string(old.Status), To: string(status),
			Outcome: string(event.State), Code: event.Result.Code, Reason: event.Result.Reason, Cause: old.Cause,
		})
	}
	for controlID, control := range next.controls {
		old, existed := controlFrom(previous, controlID)
		if existed && old.State == control.State {
			continue
		}
		from := ""
		if existed {
			from = string(old.State)
		}
		emit(observability.Transition{
			ID:   fmt.Sprintf("control/%s/revision/%d/%s", controlID, next.revision, control.State),
			Kind: observability.TransitionControlChanged, PlanID: control.PlanID, Generation: control.Generation,
			Operation: control.OperationKey, AttemptID: control.InFlightAttemptID,
			EffectID: string(control.EffectID), ReferenceID: string(control.ReferenceID), ControlID: string(control.ID),
			Provider: control.ProviderType, From: from, To: string(control.State), Outcome: string(control.State), Cause: control.Cause,
		})
	}
	for effectID, effect := range next.effects {
		old, existed := effectFrom(previous, effectID)
		if existed && old.State == effect.State && old.ExternalRevision == effect.ExternalRevision {
			continue
		}
		from := ""
		if existed {
			from = string(old.State)
		}
		emit(observability.Transition{
			ID:   fmt.Sprintf("effect/%s/revision/%d/%s", effectID, effect.ExternalRevision, effect.State),
			Kind: observability.TransitionEffectChanged, EffectID: string(effect.ID), Provider: effect.ProviderType,
			From: from, To: string(effect.State), Outcome: string(effect.State), Cause: causeForEffect(previous, next, effectID),
		})
	}
	for referenceID, reference := range next.references {
		old, existed := referenceFrom(previous, referenceID)
		if existed && old.State == reference.State {
			continue
		}
		from := ""
		if existed {
			from = string(old.State)
		}
		emit(observability.Transition{
			ID:   fmt.Sprintf("reference/%s/revision/%d/%s", referenceID, next.revision, reference.State),
			Kind: observability.TransitionEffectChanged, PlanID: reference.PlanID, Generation: reference.Generation,
			EffectID: string(reference.EffectID), ReferenceID: string(reference.ID),
			From: from, To: string(reference.State), Outcome: string(reference.State), Cause: reference.Cause,
		})
	}
}

func attemptResult(state *configExecution, id model.AttemptID, fallback model.AttemptStatus) (string, string, string) {
	if event, ok := outboxEventForAttempt(state, id); ok {
		return string(event.State), event.Result.Code, event.Result.Reason
	}
	return string(fallback), "", ""
}

func outboxEventForAttempt(state *configExecution, id model.AttemptID) (model.Event, bool) {
	if state == nil {
		return model.Event{}, false
	}
	for _, event := range state.outbox {
		if event.AttemptID == id {
			return event, true
		}
	}
	return model.Event{}, false
}

func allAttempts(state *configExecution) map[model.AttemptID]*model.Attempt {
	result := make(map[model.AttemptID]*model.Attempt)
	if state == nil {
		return result
	}
	for id, attempt := range state.attempts {
		result[id] = attempt
	}
	for id, attempt := range state.retired {
		result[id] = attempt
	}
	return result
}

func attemptContext(next, previous *configExecution, attempt *model.Attempt) (string, model.Phase) {
	for _, state := range []*configExecution{next, previous} {
		if state == nil || state.active == nil {
			continue
		}
		if node := state.active.Nodes[attempt.NodeKey]; node != nil {
			return state.active.ProviderType, node.Operation.Phase
		}
	}
	return "", ""
}

func controlFrom(state *configExecution, id ControlRequestID) (EffectControl, bool) {
	if state == nil {
		return EffectControl{}, false
	}
	value, ok := state.controls[id]
	return value, ok
}

func effectFrom(state *configExecution, id EffectID) (ActiveEffect, bool) {
	if state == nil {
		return ActiveEffect{}, false
	}
	value, ok := state.effects[id]
	return value, ok
}

func referenceFrom(state *configExecution, id ReferenceID) (EffectReference, bool) {
	if state == nil {
		return EffectReference{}, false
	}
	value, ok := state.references[id]
	return value, ok
}

func causeForEffect(previous, next *configExecution, id EffectID) model.CausalContext {
	for controlID, control := range next.controls {
		old, existed := controlFrom(previous, controlID)
		if control.EffectID == id && control.Cause != (model.CausalContext{}) && (!existed || old.State != control.State) {
			return control.Cause
		}
	}
	for referenceID, reference := range next.references {
		old, existed := referenceFrom(previous, referenceID)
		if reference.EffectID == id && reference.Cause != (model.CausalContext{}) && (!existed || old.State != reference.State) {
			return reference.Cause
		}
	}
	// An Effect may be shared. Attribute it only when all remaining owners agree;
	// otherwise leave Cause empty rather than attach the wrong workflow.
	var result model.CausalContext
	for _, reference := range next.references {
		if reference.EffectID != id || reference.Cause == (model.CausalContext{}) {
			continue
		}
		if result != (model.CausalContext{}) && result != reference.Cause {
			return model.CausalContext{}
		}
		result = reference.Cause
	}
	return result
}

func fillMissingCausalContext(state *configExecution) {
	if state == nil || state.active == nil {
		return
	}
	cause := state.active.Desired.Cause
	for _, attempt := range state.attempts {
		if attempt.Cause == (model.CausalContext{}) {
			attempt.Cause = cause
		}
	}
	for _, attempt := range state.retired {
		if attempt.Cause == (model.CausalContext{}) {
			attempt.Cause = cause
		}
	}
	for id, reference := range state.references {
		if reference.Cause == (model.CausalContext{}) {
			reference.Cause = cause
			state.references[id] = reference
		}
	}
	for id, control := range state.controls {
		if control.Cause == (model.CausalContext{}) {
			if reference := state.references[control.ReferenceID]; reference.Cause != (model.CausalContext{}) {
				control.Cause = reference.Cause
			} else {
				control.Cause = cause
			}
			state.controls[id] = control
		}
	}
	for id, event := range state.outbox {
		if event.Cause == (model.CausalContext{}) {
			if attempt := state.attempts[event.AttemptID]; attempt != nil {
				event.Cause = attempt.Cause
			} else if attempt := state.retired[event.AttemptID]; attempt != nil {
				event.Cause = attempt.Cause
			} else {
				event.Cause = cause
			}
			state.outbox[id] = event
		}
	}
}

func (r *PlanRegistry) lockConfig(id model.ConfigID) func() {
	lock, _ := r.locks.LoadOrStore(id.Name, &sync.Mutex{})
	mutex := lock.(*sync.Mutex)
	mutex.Lock()
	return mutex.Unlock
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
		if snapshot == nil || (snapshot.Plan == nil && snapshot.AcceptedDesired == nil) {
			continue
		}
		if snapshot.AcceptedDesired != nil {
			if snapshot.AcceptedDesired.ConfigID != id {
				return errors.Errorf("accepted desired belongs to %q, loaded as %q", snapshot.AcceptedDesired.ConfigID.Name, id.Name)
			}
			if err := validateDesired(*snapshot.AcceptedDesired); err != nil {
				return errors.Wrapf(err, "invalid accepted desired for %q", id.Name)
			}
		}
		migrateLegacyControlTargets(snapshot)
		if err := ValidateEffectSnapshot(*snapshot); err != nil {
			return errors.Wrapf(err, "invalid execution snapshot for %q", id.Name)
		}
		state := &configExecution{revision: snapshot.Revision, deleting: snapshot.Deleting, active: snapshot.Plan.Clone(), attempts: make(map[model.AttemptID]*model.Attempt), retired: make(map[model.AttemptID]*model.Attempt), outbox: make(map[string]model.Event), effects: make(map[EffectID]ActiveEffect), references: make(map[ReferenceID]EffectReference), controls: make(map[ControlRequestID]EffectControl)}
		if snapshot.AcceptedDesired != nil {
			desired := model.CloneDesiredState(*snapshot.AcceptedDesired)
			state.accepted = &desired
		} else if snapshot.Plan != nil {
			desired := model.CloneDesiredState(snapshot.Plan.Desired)
			state.accepted = &desired
		}
		for i := range snapshot.Attempts {
			attempt := snapshot.Attempts[i]
			copy := attempt
			if attempt.Status == model.AttemptRunning || attempt.Status == model.AttemptCancelling || attempt.Status == model.AttemptDraining {
				copy.Status = model.AttemptUnknown
				state.retired[copy.ID] = &copy
				if state.active != nil {
					if node := state.active.Nodes[copy.NodeKey]; node != nil && node.AttemptID == copy.ID {
						node.Status = model.NodeDraining
					}
				}
			} else {
				state.attempts[copy.ID] = &copy
			}
		}
		for _, event := range snapshot.Outbox {
			state.outbox[event.EventID] = cloneEvent(event)
		}
		for _, effect := range snapshot.Effects {
			state.effects[effect.ID] = effect.Clone()
		}
		for _, reference := range snapshot.EffectReferences {
			state.references[reference.ID] = reference
		}
		for _, control := range snapshot.EffectControls {
			if control.State == EffectControlInFlight {
				// Reclaim on restore: the owning process is gone. Reset to
				// Pending so the next scheduler sweep re-claims with a fresh
				// attempt; the prior attempt was already retired as Unknown.
				control.State = EffectControlPending
				control.InFlightAttemptID = ""
				control.PollRequestID = ""
				control.LeaseExpiresAt = time.Time{}
				control.RetryCount++
			}
			state.controls[control.ID] = control
		}
		r.configs[id.Name] = state
	}
	return nil
}

// migrateLegacyControlTargets upgrades snapshots written before TargetKind was
// mandatory. Complete identities are plan nodes; empty identities are
// maintenance. Partially populated identities remain invalid and fail restore.
func migrateLegacyControlTargets(snapshot *ExecutionSnapshot) {
	for i := range snapshot.EffectControls {
		control := &snapshot.EffectControls[i]
		if control.TargetKind != "" {
			continue
		}
		complete := control.PlanID != "" && control.Generation != 0 && control.OperationKey != ""
		empty := control.PlanID == "" && control.Generation == 0 && control.OperationKey == ""
		switch {
		case complete:
			control.TargetKind = EffectTargetPlanNode
		case empty:
			control.TargetKind = EffectTargetMaintenance
		}
	}
}

func cloneConfigExecution(state *configExecution) *configExecution {
	if state == nil {
		return nil
	}
	copy := &configExecution{revision: state.revision, deleting: state.deleting, active: state.active.Clone(), attempts: make(map[model.AttemptID]*model.Attempt), retired: make(map[model.AttemptID]*model.Attempt), outbox: make(map[string]model.Event), effects: make(map[EffectID]ActiveEffect), references: make(map[ReferenceID]EffectReference), controls: make(map[ControlRequestID]EffectControl)}
	if state.accepted != nil {
		desired := model.CloneDesiredState(*state.accepted)
		copy.accepted = &desired
	}
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
	for id, effect := range state.effects {
		copy.effects[id] = effect.Clone()
	}
	for id, reference := range state.references {
		copy.references[id] = reference
	}
	for id, control := range state.controls {
		copy.controls[id] = control
	}
	return copy
}

// AcceptDesired durably records the latest desired revision before planning.
// A nil error is therefore a durable acceptance ACK from the configured
// ExecutionStore, not merely confirmation that an in-memory queue had space.
func (r *PlanRegistry) AcceptDesired(ctx context.Context, desired model.DesiredState) (bool, error) {
	if err := validateDesired(desired); err != nil {
		return false, err
	}
	unlockConfig := r.lockConfig(desired.ConfigID)
	defer unlockConfig()
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.configs[desired.ConfigID.Name]
	state := cloneConfigExecution(current)
	if state == nil {
		state = &configExecution{attempts: make(map[model.AttemptID]*model.Attempt), retired: make(map[model.AttemptID]*model.Attempt), outbox: make(map[string]model.Event), effects: make(map[EffectID]ActiveEffect), references: make(map[ReferenceID]EffectReference), controls: make(map[ControlRequestID]EffectControl)}
	}
	var previous *model.DesiredState
	if state.accepted != nil {
		previous = state.accepted
	} else if state.active != nil {
		previous = &state.active.Desired
	}
	if previous != nil {
		if desired.Version < previous.Version || (desired.Version == previous.Version && desired.Digest != previous.Digest) {
			return false, ErrDesiredConflict
		}
		if desired.Version == previous.Version && desired.Digest == previous.Digest {
			if !sameDesired(*previous, desired) {
				return false, ErrDesiredConflict
			}
			return false, nil
		}
	}
	copy := model.CloneDesiredState(desired)
	state.accepted = &copy
	if err := r.persistLocked(ctx, desired.ConfigID, state.revision, state); err != nil {
		return false, err
	}
	r.configs[desired.ConfigID.Name] = state
	return true, nil
}

func sameDesired(a, b model.DesiredState) bool {
	return a.ConfigID == b.ConfigID && a.ProviderType == b.ProviderType &&
		a.Version == b.Version && a.Digest == b.Digest && bytes.Equal(a.Spec, b.Spec) &&
		slices.Equal(a.DependsOn, b.DependsOn)
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
	unlockConfig := r.lockConfig(candidate.ConfigID)
	defer unlockConfig()
	r.mu.Lock()
	defer r.mu.Unlock()
	original := r.configs[candidate.ConfigID.Name]
	state := cloneConfigExecution(original)
	if state == nil {
		state = &configExecution{attempts: make(map[model.AttemptID]*model.Attempt), retired: make(map[model.AttemptID]*model.Attempt), outbox: make(map[string]model.Event), effects: make(map[EffectID]ActiveEffect), references: make(map[ReferenceID]EffectReference), controls: make(map[ControlRequestID]EffectControl)}
	}
	current := model.Generation(0)
	if state.active != nil {
		current = state.active.Generation
	}
	if state.accepted != nil && (candidate.DesiredVersion != state.accepted.Version || candidate.DesiredDigest != state.accepted.Digest) {
		return nil, PlanChange{}, ErrDesiredConflict
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
		for _, key := range change.Carry {
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
	// Transfer effect references for ensure/observe/release operations.
	if err := r.transferEffectReferences(ctx, state, state.active, installed, change); err != nil {
		return nil, PlanChange{}, err
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

// transferEffectReferences creates, carries, or releases EffectReferences based
// on plan change classification. It handles same-artifact reuse (carry reference
// to new generation), changed-artifact release, and new effect creation.
func (r *PlanRegistry) transferEffectReferences(ctx context.Context, state *configExecution, oldPlan, installed *model.Plan, change PlanChange) error {
	if installed == nil {
		return nil
	}
	// Collect all effect ensure operations by EffectKey.
	type ensureInfo struct {
		key            model.OperationKey
		effectKey      string
		fingerprint    string
		artifactID     string
		providerType   string
		providerDigest string
	}
	newEnsures := make(map[string]ensureInfo)
	for key, node := range installed.Nodes {
		op := node.Operation
		if op.ExecutionKind != model.ExecutionEffectEnsure {
			continue
		}
		newEnsures[op.EffectKey] = ensureInfo{
			key: key, effectKey: op.EffectKey,
			fingerprint: op.Fingerprint, artifactID: installed.DesiredDigest,
			providerType: installed.ProviderType, providerDigest: installed.ProviderDigest,
		}
	}

	// Same-artifact carry: reuse Bound effect via EnsureReference. Changed
	// artifact: release the old reference; DAG ensure creates effect B.
	for _, info := range newEnsures {
		newRefID := ReferenceID(fmt.Sprintf("%s/%s/%d/%s", installed.ConfigID.Name, installed.ID, installed.Generation, info.effectKey))
		var oldRef *EffectReference
		for id := range state.references {
			ref := state.references[id]
			if ref.EffectKey != info.effectKey {
				continue
			}
			if ref.ConfigID.Name != installed.ConfigID.Name {
				continue
			}
			if ref.State == EffectReferenceReleased || ref.State == EffectReferenceReleaseRequested {
				continue
			}
			oldRef = &ref
			break
		}
		if oldRef == nil {
			continue
		}
		oldEffect := state.effects[oldRef.EffectID]
		sameArtifact := oldEffect.SemanticFingerprint == info.fingerprint && oldEffect.ArtifactID == info.artifactID
		if !sameArtifact {
			if oldRef.State == EffectReferenceActive || oldRef.State == EffectReferenceEnsuring {
				old := *oldRef
				old.State = EffectReferenceReleaseRequested
				state.references[old.ID] = old
				retireIncompatibleControlsLocked(state, old.ID)
				releaseControlID := ControlRequestID("release-" + string(old.ID))
				if _, exists := state.controls[releaseControlID]; !exists {
					control := EffectControl{
						ID: releaseControlID, ConfigID: installed.ConfigID,
						ProviderType: oldEffect.ProviderType, ProviderDigest: oldEffect.ProviderDigest,
						Kind: EffectControlRelease, State: EffectControlPending,
						TargetKind: EffectTargetPlanNode,
						EffectID:   old.EffectID, ReferenceID: old.ID,
						PlanID: installed.ID, Generation: installed.Generation,
						OperationKey: findEffectOperationKey(installed, info.effectKey, model.ExecutionEffectRelease),
						NextCheckAt:  time.Now(),
					}
					bindControlToPlanNodeOrMaintenance(&control, installed, info.effectKey, model.ExecutionEffectRelease)
					state.controls[releaseControlID] = control
				}
			}
			continue
		}
		if oldEffect.Binding != EffectBindingBound || oldEffect.ExternalJobID == "" {
			continue
		}
		if _, exists := state.references[newRefID]; exists {
			continue
		}
		newRef := EffectReference{
			ID: newRefID, EffectID: oldRef.EffectID,
			ConfigID: installed.ConfigID, PlanID: installed.ID,
			Generation: installed.Generation, EffectKey: info.effectKey,
			State: EffectReferenceEnsuring,
		}
		state.references[newRef.ID] = newRef
		ensureRefControlID := ControlRequestID("ensure-ref-" + string(newRef.ID))
		if _, exists := state.controls[ensureRefControlID]; !exists {
			control := EffectControl{
				ID: ensureRefControlID, ConfigID: installed.ConfigID,
				ProviderType: oldEffect.ProviderType, ProviderDigest: oldEffect.ProviderDigest,
				Kind: EffectControlEnsureReference, State: EffectControlPending,
				TargetKind: EffectTargetPlanNode,
				EffectID:   oldRef.EffectID, ReferenceID: newRef.ID,
				PlanID: installed.ID, Generation: installed.Generation,
				OperationKey: findEffectOperationKey(installed, info.effectKey, model.ExecutionEffectEnsure),
				NextCheckAt:  time.Now(),
			}
			bindControlToPlanNodeOrMaintenance(&control, installed, info.effectKey, model.ExecutionEffectEnsure)
			state.controls[ensureRefControlID] = control
		}
	}

	// For dropped ensure nodes with existing references: mark for release.
	if oldPlan != nil {
		for _, key := range change.Drop {
			oldNode := oldPlan.Nodes[key]
			if oldNode == nil || oldNode.Operation.ExecutionKind != model.ExecutionEffectEnsure {
				continue
			}
			effectKey := oldNode.Operation.EffectKey
			if _, exists := newEnsures[effectKey]; exists {
				continue
			}
			expectedRefID := ReferenceID(fmt.Sprintf("%s/%s/%d/%s", installed.ConfigID.Name, oldPlan.ID, oldPlan.Generation, effectKey))
			if ref, exists := state.references[expectedRefID]; exists {
				ref.State = EffectReferenceReleaseRequested
				state.references[ref.ID] = ref
				retireIncompatibleControlsLocked(state, ref.ID)
				releaseControlID := ControlRequestID("release-" + string(ref.ID))
				if _, exists := state.controls[releaseControlID]; !exists {
					control := EffectControl{
						ID: releaseControlID, ConfigID: installed.ConfigID,
						ProviderType: installed.ProviderType, ProviderDigest: installed.ProviderDigest,
						Kind: EffectControlRelease, State: EffectControlPending,
						TargetKind: EffectTargetPlanNode,
						EffectID:   ref.EffectID, ReferenceID: ref.ID,
						PlanID: installed.ID, Generation: installed.Generation,
						OperationKey: findEffectOperationKey(installed, effectKey, model.ExecutionEffectRelease),
						NextCheckAt:  time.Now(),
					}
					bindControlToPlanNodeOrMaintenance(&control, installed, effectKey, model.ExecutionEffectRelease)
					state.controls[releaseControlID] = control
				}
			}
		}
	}
	return nil
}

// StartAttempt atomically transitions one pending/ready node to Running.
func (r *PlanRegistry) StartAttempt(ctx context.Context, configID model.ConfigID, generation model.Generation, key model.OperationKey, attemptID model.AttemptID) (*model.Attempt, error) {
	if attemptID == "" {
		return nil, errors.New("attempt ID is empty")
	}
	unlockConfig := r.lockConfig(configID)
	defer unlockConfig()
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
	attempt := &model.Attempt{ID: attemptID, PlanID: state.active.ID, Generation: generation, ConfigID: configID, NodeKey: key, Fingerprint: node.Operation.Fingerprint, ConflictKey: node.Operation.ConflictKey, Status: model.AttemptRunning, Cause: state.active.Desired.Cause}
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
	unlockConfig := r.lockConfig(model.ConfigID{Name: event.ConfigID})
	defer unlockConfig()
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
	return status == model.AttemptCompleted || status == model.AttemptFailed || status == model.AttemptCancelled || status == model.AttemptYielded
}

// ReadyOperations returns dependency-ready nodes not blocked by retired
// attempts or unresolved external effects.
func (r *PlanRegistry) ReadyOperations(configID model.ConfigID) (*model.Plan, []model.Operation) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	state := r.configs[configID.Name]
	if state == nil || state.active == nil || state.deleting {
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
		if !dependenciesDone {
			continue
		}
		// Control scheduler owns these once a durable control is pending.
		if node.Operation.ExecutionKind == model.ExecutionEffectEnsure &&
			r.hasActiveControlLocked(state, node.Operation.EffectKey, EffectControlEnsureReference) {
			continue
		}
		if node.Operation.ExecutionKind == model.ExecutionEffectObserve &&
			r.hasActiveControlLocked(state, node.Operation.EffectKey, EffectControlObserve) {
			continue
		}
		if node.Operation.ExecutionKind == model.ExecutionEffectRelease &&
			r.hasActiveControlLocked(state, node.Operation.EffectKey, EffectControlRelease) {
			continue
		}
		if r.operationBlockedByEffectsLocked(state, node.Operation) {
			continue
		}
		ready = append(ready, node.Operation)
	}
	return state.active.Clone(), ready
}

func (r *PlanRegistry) hasActiveControlLocked(state *configExecution, effectKey string, kind EffectControlKind) bool {
	for _, control := range state.controls {
		if control.Kind != kind || control.State == EffectControlCompleted {
			continue
		}
		if ref := state.references[control.ReferenceID]; ref.EffectKey == effectKey {
			return true
		}
	}
	return false
}

func (r *PlanRegistry) operationBlockedByEffectsLocked(state *configExecution, operation model.Operation) bool {
	plan := state.active
	for _, effect := range state.effects {
		ref, ok := state.references[referenceForEffectLocked(state, effect.ID, plan)]
		if !ok {
			// Fall back: any reference pointing at this effect.
			for _, candidate := range state.references {
				if candidate.EffectID == effect.ID {
					ref = candidate
					ok = true
					break
				}
			}
		}
		if !ok {
			continue
		}
		if OperationBlockedByEffect(operation, plan, effect, ref) {
			return true
		}
	}
	return false
}

func referenceForEffectLocked(state *configExecution, effectID EffectID, plan *model.Plan) ReferenceID {
	if plan == nil {
		return ""
	}
	for id, ref := range state.references {
		if ref.EffectID == effectID && ref.PlanID == plan.ID && ref.Generation == plan.Generation {
			return id
		}
	}
	return ""
}

// LookupEffectBinding returns the durable effect/reference for a plan effect slot.
func (r *PlanRegistry) LookupEffectBinding(configID model.ConfigID, planID model.PlanID, generation model.Generation, effectKey string) (ActiveEffect, EffectReference, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	state := r.configs[configID.Name]
	if state == nil {
		return ActiveEffect{}, EffectReference{}, false
	}
	refID := newReferenceID(configID, planID, generation, effectKey)
	ref, ok := state.references[refID]
	if !ok {
		return ActiveEffect{}, EffectReference{}, false
	}
	effect, ok := state.effects[ref.EffectID]
	if !ok {
		return ActiveEffect{}, EffectReference{}, false
	}
	return effect, ref, true
}

// LookupReference returns one durable effect reference by ID.
func (r *PlanRegistry) LookupReference(configID model.ConfigID, referenceID ReferenceID) (EffectReference, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	state := r.configs[configID.Name]
	if state == nil {
		return EffectReference{}, false
	}
	ref, ok := state.references[referenceID]
	return ref, ok
}

// LookupEffect returns one durable effect by ID.
func (r *PlanRegistry) LookupEffect(configID model.ConfigID, effectID EffectID) (ActiveEffect, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	state := r.configs[configID.Name]
	if state == nil {
		return ActiveEffect{}, false
	}
	effect, ok := state.effects[effectID]
	return effect, ok
}

// EnqueueOutbox persists an event before any best-effort publication.
func (r *PlanRegistry) EnqueueOutbox(ctx context.Context, event model.Event) error {
	if event.EventID == "" {
		return errors.New("outbox event ID is empty")
	}
	unlockConfig := r.lockConfig(model.ConfigID{Name: event.ConfigID})
	defer unlockConfig()
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

// RuntimeCounts returns a detached, point-in-time view for aggregate gauges.
// The observer is invoked by Reconciler only after this registry lock is gone.
func (r *PlanRegistry) RuntimeCounts() (map[model.AttemptStatus]int64, map[string]int64, int64) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	attempts := make(map[model.AttemptStatus]int64)
	controls := make(map[string]int64)
	var outbox int64
	for _, state := range r.configs {
		for _, attempt := range state.attempts {
			attempts[attempt.Status]++
		}
		for _, attempt := range state.retired {
			attempts[attempt.Status]++
		}
		for _, control := range state.controls {
			if control.State != EffectControlCompleted {
				controls[string(control.Kind)]++
			}
		}
		outbox += int64(len(state.outbox))
	}
	return attempts, controls, outbox
}

// AckOutbox removes an event after successful processing.
func (r *PlanRegistry) AckOutbox(ctx context.Context, configID model.ConfigID, eventID string) error {
	unlockConfig := r.lockConfig(configID)
	defer unlockConfig()
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
	unlockConfig := r.lockConfig(configID)
	defer unlockConfig()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.store != nil {
		r.mu.Unlock()
		err := r.store.DeleteExecution(ctx, configID)
		r.mu.Lock()
		if err != nil {
			return err
		}
	}
	delete(r.configs, configID.Name)
	return nil
}
