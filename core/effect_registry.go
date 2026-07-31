package core

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/akzj/converge/pkg/model"
)

type BeginEnsureRequest struct {
	Identity TransitionIdentity
	Spec     ImmutableEnsureSpec
}

type BeginReleaseRequest struct {
	Identity TransitionIdentity
}

type ImmutableEnsureSpec struct {
	IdempotencyKey, ArtifactID, SemanticFingerprint string
	EnsureSpec                                      []byte
}

type TransitionIdentity struct {
	EffectIdentity EffectIdentity
	AttemptID      model.AttemptID
	RequestID      ControlRequestID
}

// BeginEnsureEffect persists an Effect in Ensuring state, an EffectReference in
// Ensuring state, and an EnsureRetry control. All identity and spec fields are
// immutable after this command.
func (r *PlanRegistry) BeginEnsureEffect(ctx context.Context, req BeginEnsureRequest) (TransitionDisposition, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.configs[req.Identity.EffectIdentity.ConfigID.Name]
	state := cloneConfigExecution(current)
	if state == nil {
		return TransitionRejected, errors.New("config not found")
	}
	if existing, exists := state.effects[req.Identity.EffectIdentity.EffectID]; exists {
		if existing.IdempotencyKey == req.Spec.IdempotencyKey &&
			existing.ArtifactID == req.Spec.ArtifactID &&
			existing.SemanticFingerprint == req.Spec.SemanticFingerprint &&
			string(existing.EnsureSpec) == string(req.Spec.EnsureSpec) {
			return TransitionDuplicate, nil
		}
		return TransitionRejected, errors.New("effect id reuse with different immutable ensure spec")
	}
	if _, exists := state.references[req.Identity.EffectIdentity.ReferenceID]; exists {
		return TransitionDuplicate, nil
	}
	effect := ActiveEffect{
		ID: req.Identity.EffectIdentity.EffectID, Binding: EffectBindingUnbound,
		ArtifactID: req.Spec.ArtifactID, IdempotencyKey: req.Spec.IdempotencyKey,
		SemanticFingerprint: req.Spec.SemanticFingerprint,
		EnsureSpec:          append([]byte(nil), req.Spec.EnsureSpec...),
		ProviderType:        req.Identity.EffectIdentity.ProviderType,
		ProviderDigest:      req.Identity.EffectIdentity.ProviderDigest,
		ConflictKey:         effectSlotConflictKey(req.Identity.EffectIdentity.ConfigID, req.Identity.EffectIdentity.EffectKey),
		State:               ExternalEffectEnsuring,
		ResolutionRequired:  true,
	}
	reference := EffectReference{
		ID: req.Identity.EffectIdentity.ReferenceID, EffectID: req.Identity.EffectIdentity.EffectID,
		ConfigID: req.Identity.EffectIdentity.ConfigID, PlanID: req.Identity.EffectIdentity.PlanID,
		Generation: req.Identity.EffectIdentity.Generation, EffectKey: req.Identity.EffectIdentity.EffectKey,
		State: EffectReferenceEnsuring,
	}
	control := EffectControl{
		ID: req.Identity.RequestID, ConfigID: req.Identity.EffectIdentity.ConfigID,
		ProviderType: effect.ProviderType, ProviderDigest: effect.ProviderDigest,
		Kind: EffectControlEnsureRetry, State: EffectControlPending,
		EffectID: req.Identity.EffectIdentity.EffectID, ReferenceID: req.Identity.EffectIdentity.ReferenceID,
		PlanID: req.Identity.EffectIdentity.PlanID, Generation: req.Identity.EffectIdentity.Generation,
		OperationKey: req.Identity.EffectIdentity.OperationKey,
		NextCheckAt:  time.Now(),
	}
	state.effects[effect.ID] = effect
	state.references[reference.ID] = reference
	state.controls[control.ID] = control
	if err := r.persistLocked(ctx, req.Identity.EffectIdentity.ConfigID, state.revision, state); err != nil {
		return TransitionRejected, err
	}
	r.configs[req.Identity.EffectIdentity.ConfigID.Name] = state
	return TransitionApplied, nil
}

// ApplyEnsureResult applies an Ensure disposition to an unbound effect.
// Bound success becomes Active or CancelRequested Bound (late ensure after delete).
// Unknown/Failed outcomes stay unbound and retain EnsureRetry when retryable.
func (r *PlanRegistry) ApplyEnsureResult(ctx context.Context, identity TransitionIdentity, result EnsureEffectResult) (TransitionDisposition, error) {
	if err := ValidateEnsureDispositionFailure(result.Disposition, result.Failure); err != nil {
		return TransitionRejected, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.configs[identity.EffectIdentity.ConfigID.Name]
	state := cloneConfigExecution(current)
	if state == nil {
		return TransitionRejected, nil
	}
	effect := state.effects[identity.EffectIdentity.EffectID]
	if effect.ID == "" {
		return TransitionStale, nil
	}
	if result.EffectID != "" && result.EffectID != effect.ID {
		return TransitionRejected, errors.New("ensure result effect id mismatch")
	}
	if result.ReferenceID != "" && result.ReferenceID != identity.EffectIdentity.ReferenceID {
		return TransitionRejected, errors.New("ensure result reference id mismatch")
	}
	if effect.Binding == EffectBindingBound {
		if effect.ExternalJobID == result.ExternalJobID && result.Disposition == EnsureBound {
			return TransitionDuplicate, nil
		}
		return TransitionRejected, errors.New("effect already bound")
	}
	switch effect.State {
	case ExternalEffectEnsuring, ExternalEffectUnknown, ExternalEffectCancelRequested:
	default:
		return TransitionDuplicate, nil
	}

	oldEffect := effect
	oldReference := state.references[identity.EffectIdentity.ReferenceID]
	reference := oldReference

	switch result.Disposition {
	case EnsureBound:
		if result.ExternalJobID == "" || result.ExternalRevision == 0 {
			return TransitionRejected, errors.New("bound ensure result missing job identity")
		}
		effect.Binding = EffectBindingBound
		effect.ExternalJobID = result.ExternalJobID
		effect.ExternalRevision = result.ExternalRevision
		effect.ResolutionRequired = true
		if oldEffect.State == ExternalEffectCancelRequested {
			effect.State = ExternalEffectCancelRequested
		} else {
			effect.State = ExternalEffectActive
		}
		if err := ValidateEffectTransition(oldEffect, effect, EffectTransitionEnsureResult); err != nil {
			return TransitionRejected, err
		}
		state.effects[effect.ID] = effect

		if effect.State == ExternalEffectCancelRequested {
			if reference.ID != "" && reference.State != EffectReferenceReleased {
				nextRef := reference
				if nextRef.State != EffectReferenceReleaseRequested {
					nextRef.State = EffectReferenceReleaseRequested
					if err := ValidateReferenceTransition(reference, nextRef); err != nil {
						return TransitionRejected, err
					}
					reference = nextRef
					state.references[reference.ID] = reference
				}
				retireObserveControlsLocked(state, reference.ID)
				releaseID := ControlRequestID("release-" + string(reference.ID))
				if _, exists := state.controls[releaseID]; !exists {
					state.controls[releaseID] = EffectControl{
						ID: releaseID, ConfigID: identity.EffectIdentity.ConfigID,
						ProviderType: effect.ProviderType, ProviderDigest: effect.ProviderDigest,
						Kind: EffectControlRelease, State: EffectControlPending,
						EffectID: effect.ID, ReferenceID: reference.ID,
						PlanID: identity.EffectIdentity.PlanID, Generation: identity.EffectIdentity.Generation,
						OperationKey: findEffectOperationKey(state.active, identity.EffectIdentity.EffectKey, model.ExecutionEffectRelease),
						NextCheckAt:  time.Now(),
					}
				}
			}
			delete(state.controls, identity.RequestID)
		} else {
			if reference.ID != "" {
				nextRef := reference
				nextRef.State = EffectReferenceActive
				if err := ValidateReferenceTransition(reference, nextRef); err != nil {
					return TransitionRejected, err
				}
				state.references[nextRef.ID] = nextRef
			}
			observeControl := EffectControl{
				ID:       ControlRequestID("observe-" + string(identity.EffectIdentity.ReferenceID)),
				ConfigID: identity.EffectIdentity.ConfigID, ProviderType: effect.ProviderType,
				ProviderDigest: effect.ProviderDigest, Kind: EffectControlObserve,
				State: EffectControlPending, EffectID: effect.ID, ReferenceID: identity.EffectIdentity.ReferenceID,
				PlanID: identity.EffectIdentity.PlanID, Generation: identity.EffectIdentity.Generation,
				OperationKey: findEffectOperationKey(state.active, identity.EffectIdentity.EffectKey, model.ExecutionEffectObserve),
				NextCheckAt:  time.Now(),
			}
			delete(state.controls, identity.RequestID)
			state.controls[observeControl.ID] = observeControl
		}

	case EnsureUnknown:
		effect.State = ExternalEffectUnknown
		effect.ResolutionRequired = true
		if err := ValidateEffectTransition(oldEffect, effect, EffectTransitionEnsureResult); err != nil {
			return TransitionRejected, err
		}
		state.effects[effect.ID] = effect
		if control, ok := state.controls[identity.RequestID]; ok {
			control.State = EffectControlPending
			control.InFlightAttemptID = ""
			control.PollRequestID = ""
			control.LeaseExpiresAt = time.Time{}
			control.NextCheckAt = time.Now().Add(time.Second)
			control.RetryCount++
			state.controls[control.ID] = control
		}

	case EnsureFailed:
		effect.State = ExternalEffectFailed
		if result.Failure == EnsureFailureAuthoritativeRejected {
			effect.ResolutionRequired = false
		} else {
			effect.ResolutionRequired = true
		}
		if err := ValidateEffectTransition(oldEffect, effect, EffectTransitionEnsureResult); err != nil {
			return TransitionRejected, err
		}
		state.effects[effect.ID] = effect
		delete(state.controls, identity.RequestID)

	default:
		return TransitionRejected, errors.Errorf("unhandled ensure disposition %q", result.Disposition)
	}

	if err := r.persistLocked(ctx, identity.EffectIdentity.ConfigID, state.revision, state); err != nil {
		return TransitionRejected, err
	}
	r.configs[identity.EffectIdentity.ConfigID.Name] = state
	return TransitionApplied, nil
}

// CompleteEnsureAndNode atomically applies an EnsureBound result, marks the
// ensure node terminal, and enqueues the lifecycle outbox event in a single
// execution Revision CAS. It closes the crash window where the effect is Bound
// but the ensure node has not advanced.
func (r *PlanRegistry) CompleteEnsureAndNode(
	ctx context.Context,
	identity TransitionIdentity,
	result EnsureEffectResult,
	nodeKey model.OperationKey,
	event model.Event,
) (TransitionDisposition, error) {
	if err := ValidateEnsureDispositionFailure(result.Disposition, result.Failure); err != nil {
		return TransitionRejected, err
	}
	if result.Disposition != EnsureBound {
		return TransitionRejected, errors.Errorf("unsupported terminal ensure disposition %q", result.Disposition)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.configs[identity.EffectIdentity.ConfigID.Name]
	state := cloneConfigExecution(current)
	if state == nil || state.active == nil {
		return TransitionRejected, errors.New("config not found")
	}
	effect := state.effects[identity.EffectIdentity.EffectID]
	if effect.ID == "" {
		return TransitionStale, nil
	}
	if result.EffectID != "" && result.EffectID != effect.ID {
		return TransitionRejected, errors.New("ensure result effect id mismatch")
	}
	if effect.Binding == EffectBindingBound {
		if effect.ExternalJobID == result.ExternalJobID {
			return TransitionDuplicate, nil
		}
		return TransitionRejected, errors.New("effect already bound")
	}
	switch effect.State {
	case ExternalEffectEnsuring, ExternalEffectUnknown, ExternalEffectCancelRequested:
	default:
		return TransitionDuplicate, nil
	}

	oldEffect := effect
	effect.Binding = EffectBindingBound
	effect.ExternalJobID = result.ExternalJobID
	effect.ExternalRevision = result.ExternalRevision
	effect.ResolutionRequired = true
	if oldEffect.State == ExternalEffectCancelRequested {
		effect.State = ExternalEffectCancelRequested
	} else {
		effect.State = ExternalEffectActive
	}
	if err := ValidateEffectTransition(oldEffect, effect, EffectTransitionEnsureResult); err != nil {
		return TransitionRejected, err
	}
	state.effects[effect.ID] = effect

	reference := state.references[identity.EffectIdentity.ReferenceID]
	if reference.ID != "" {
		oldReference := reference
		if reference.State != EffectReferenceActive {
			reference.State = EffectReferenceActive
			if err := ValidateReferenceTransition(oldReference, reference); err != nil {
				return TransitionRejected, err
			}
		}
		state.references[reference.ID] = reference
	}

	// Create the Observe control so the scheduler can drive polling.
	observeControl := EffectControl{
		ID:       ControlRequestID("observe-" + string(identity.EffectIdentity.ReferenceID)),
		ConfigID: identity.EffectIdentity.ConfigID, ProviderType: effect.ProviderType,
		ProviderDigest: effect.ProviderDigest, Kind: EffectControlObserve,
		State: EffectControlPending, EffectID: effect.ID, ReferenceID: identity.EffectIdentity.ReferenceID,
		PlanID: state.active.ID, Generation: state.active.Generation,
		OperationKey: findEffectOperationKey(state.active, identity.EffectIdentity.EffectKey, model.ExecutionEffectObserve),
		NextCheckAt:  time.Now(),
	}
	delete(state.controls, identity.RequestID)
	state.controls[observeControl.ID] = observeControl

	// --- Verify NodeIdentity matches the active plan, preventing
	// cross-generation advancement by a stale control. ---
	if identity.EffectIdentity.PlanID != "" && identity.EffectIdentity.PlanID != state.active.ID {
		return TransitionStale, nil
	}
	if identity.EffectIdentity.Generation != 0 && identity.EffectIdentity.Generation != state.active.Generation {
		return TransitionStale, nil
	}

	// --- Mark the plan node terminal and retire the control attempt ---
	node := state.active.Nodes[nodeKey]
	if node == nil {
		return TransitionRejected, errors.Errorf("operation %q not found", nodeKey)
	}
	if node.Status != model.NodeCompleted {
		attempt := &model.Attempt{
			ID: identity.AttemptID, PlanID: state.active.ID, Generation: state.active.Generation,
			ConfigID: identity.EffectIdentity.ConfigID, NodeKey: nodeKey,
			Fingerprint: node.Operation.Fingerprint, ConflictKey: node.Operation.ConflictKey,
			Status: model.AttemptCompleted,
		}
		node.Status = model.NodeCompleted
		node.AttemptID = identity.AttemptID
		state.retired[identity.AttemptID] = attempt
		delete(state.attempts, identity.AttemptID)
	}

	// --- Enqueue the lifecycle outbox event in the same CAS ---
	if event.EventID == "" {
		return TransitionRejected, errors.New("outbox event ID is empty")
	}
	state.outbox[event.EventID] = event

	if err := r.persistLocked(ctx, identity.EffectIdentity.ConfigID, state.revision, state); err != nil {
		return TransitionRejected, err
	}
	r.configs[identity.EffectIdentity.ConfigID.Name] = state
	return TransitionApplied, nil
}


// ClaimDueControl atomically transitions a Pending/Yielded control to InFlight
// with the given AttemptID, PollRequestID, and lease expiration, and creates a
// complete Attempt record so control polls share the global attempt history.
func (r *PlanRegistry) ClaimDueControl(ctx context.Context, configID model.ConfigID, controlID ControlRequestID, now time.Time, attemptID model.AttemptID, pollID PollRequestID, leaseUntil time.Time) (*EffectControl, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.configs[configID.Name]
	state := cloneConfigExecution(current)
	if state == nil {
		return nil, errors.New("config not found")
	}
	control := state.controls[controlID]
	if control.ID == "" {
		return nil, errors.Errorf("control %q not found", controlID)
	}
	if control.State != EffectControlPending && control.State != EffectControlYielded {
		return nil, errors.Errorf("control %q not claimable: %s", controlID, control.State)
	}
	if control.NextCheckAt.After(now) {
		return nil, errors.Errorf("control %q not yet due", controlID)
	}
	oldControl := control
	control.State = EffectControlInFlight
	control.InFlightAttemptID = attemptID
	control.PollRequestID = pollID
	control.LeaseExpiresAt = leaseUntil
	if err := ValidateControlTransition(oldControl, control); err != nil {
		return nil, err
	}
	// Create a complete Attempt record for the poll so it appears in the global
	// attempt history with the same identity semantics as DAG attempts.
	var fingerprint, conflictKey string
	if control.OperationKey != "" && state.active != nil {
		if node := state.active.Nodes[control.OperationKey]; node != nil {
			fingerprint, conflictKey = node.Operation.Fingerprint, node.Operation.ConflictKey
		}
	}
	if _, exists := state.attempts[attemptID]; !exists {
		state.attempts[attemptID] = &model.Attempt{
			ID: attemptID, PlanID: control.PlanID, Generation: control.Generation,
			ConfigID: control.ConfigID, NodeKey: control.OperationKey,
			Fingerprint: fingerprint, ConflictKey: conflictKey,
			Status: model.AttemptRunning, StartedAt: now, UpdatedAt: now,
		}
	}
	state.controls[control.ID] = control
	if err := r.persistLocked(ctx, configID, state.revision, state); err != nil {
		return nil, err
	}
	r.configs[configID.Name] = state
	return &control, nil
}

// ApplyEffectObservation transitions an InFlight control and its associated
// Effect state based on the provider observation.
func (r *PlanRegistry) ApplyEffectObservation(ctx context.Context, identity TransitionIdentity, observation EffectObservation) (TransitionDisposition, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.configs[identity.EffectIdentity.ConfigID.Name]
	state := cloneConfigExecution(current)
	if state == nil {
		return TransitionRejected, nil
	}
	control := state.controls[identity.RequestID]
	if control.ID == "" || control.State != EffectControlInFlight {
		return TransitionStale, nil
	}
	if observation.PollRequestID != "" && observation.PollRequestID != control.PollRequestID {
		return TransitionRejected, errors.New("poll request id mismatch")
	}
	if observation.AttemptID != "" && observation.AttemptID != control.InFlightAttemptID {
		return TransitionRejected, errors.New("attempt id mismatch")
	}
	if identity.AttemptID != "" && identity.AttemptID != control.InFlightAttemptID {
		return TransitionRejected, errors.New("attempt id mismatch")
	}
	if identity.EffectIdentity.EffectID != "" && identity.EffectIdentity.EffectID != control.EffectID {
		return TransitionRejected, errors.New("effect id mismatch")
	}
	if identity.EffectIdentity.ReferenceID != "" && identity.EffectIdentity.ReferenceID != control.ReferenceID {
		return TransitionRejected, errors.New("reference id mismatch")
	}
	effect := state.effects[identity.EffectIdentity.EffectID]
	if effect.ID == "" {
		return TransitionStale, nil
	}
	if observation.EffectID != "" && observation.EffectID != effect.ID {
		return TransitionRejected, errors.New("observation effect id mismatch")
	}
	reference := state.references[identity.EffectIdentity.ReferenceID]
	if reference.ID == "" && observation.Disposition != DispositionAbsent && observation.Disposition != DispositionAuthoritativeGone {
		return TransitionStale, nil
	}
	oldEffect, oldControl := effect, control
	switch observation.Disposition {
	case DispositionStillActive:
		next := observation.NextCheckAt
		if next.IsZero() {
			next = time.Now().Add(5 * time.Second)
		}
		control.State = EffectControlYielded
		control.NextCheckAt = next
		control.InFlightAttemptID = ""
		control.PollRequestID = ""
		control.LeaseExpiresAt = time.Time{}
		// Cancellation observation must retain Cancelling/CancelRequested.
		if control.Kind != EffectControlObserveCancellation {
			effect.State = ExternalEffectActive
		}
		effect.ExternalRevision = observation.ExternalRevision
		if err := ValidateControlTransition(oldControl, control); err != nil {
			return TransitionRejected, err
		}
		if err := ValidateEffectTransition(oldEffect, effect, EffectTransitionExternalObservation); err != nil {
			return TransitionRejected, err
		}
		state.controls[control.ID] = control
		state.effects[effect.ID] = effect

	case DispositionCompleted, DispositionCancelled:
		control.State = EffectControlCompleted
		control.InFlightAttemptID = ""
		control.PollRequestID = ""
		control.LeaseExpiresAt = time.Time{}
		effect.State = ExternalEffectCompleted
		if observation.Disposition == DispositionCancelled {
			effect.State = ExternalEffectCancelled
		}
		effect.ResolutionRequired = false
		effect.ExternalRevision = observation.ExternalRevision
		if err := ValidateControlTransition(oldControl, control); err != nil {
			return TransitionRejected, err
		}
		if err := ValidateEffectTransition(oldEffect, effect, EffectTransitionExternalObservation); err != nil {
			return TransitionRejected, err
		}
		state.controls[control.ID] = control
		state.effects[effect.ID] = effect

	case DispositionFailed:
		control.State = EffectControlCompleted
		control.InFlightAttemptID = ""
		control.PollRequestID = ""
		control.LeaseExpiresAt = time.Time{}
		effect.State = ExternalEffectFailed
		effect.ResolutionRequired = true
		if observation.ExternalRevision > 0 {
			effect.ExternalRevision = observation.ExternalRevision
		}
		if err := ValidateControlTransition(oldControl, control); err != nil {
			return TransitionRejected, err
		}
		if err := ValidateEffectTransition(oldEffect, effect, EffectTransitionExternalObservation); err != nil {
			return TransitionRejected, err
		}
		state.controls[control.ID] = control
		state.effects[effect.ID] = effect

	case DispositionAbsent:
		// Non-authoritative absence in observation. Apply yields; caller handles.
		return TransitionRejected, errors.New("non-authoritative absent observation")

	case DispositionAuthoritativeGone:
		// Authoritative Gone: remove effect and its references; fail closed until
		// desired recreates via Ensure if still needed.
		if effect.Binding != EffectBindingBound {
			return TransitionRejected, errors.New("authoritative gone requires bound effect")
		}
		if observation.ExternalJobID != "" && observation.ExternalJobID != effect.ExternalJobID {
			return TransitionRejected, errors.New("authoritative gone job id mismatch")
		}
		control.State = EffectControlCompleted
		control.InFlightAttemptID = ""
		control.PollRequestID = ""
		control.LeaseExpiresAt = time.Time{}
		if err := ValidateControlTransition(oldControl, control); err != nil {
			return TransitionRejected, err
		}
		state.controls[control.ID] = control
		delete(state.effects, effect.ID)
		for id, ref := range state.references {
			if ref.EffectID != effect.ID {
				continue
			}
			delete(state.references, id)
			for cid, c := range state.controls {
				if c.ReferenceID == id || c.EffectID == effect.ID {
					c.State = EffectControlCompleted
					c.InFlightAttemptID = ""
					c.PollRequestID = ""
					c.LeaseExpiresAt = time.Time{}
					state.controls[cid] = c
				}
			}
		}

	default:
		return TransitionRejected, errors.Errorf("unhandled observation disposition %q", observation.Disposition)
	}
	// Retire the poll attempt: yield for StillActive, complete/fail for
	// terminal dispositions.
	if observation.Disposition != DispositionStillActive {
		status := model.AttemptCompleted
		if observation.Disposition == DispositionFailed || observation.Disposition == DispositionAuthoritativeGone {
			status = model.AttemptFailed
		}
		retireControlAttemptLocked(state, identity.AttemptID, status)
	} else {
		retireControlAttemptLocked(state, identity.AttemptID, model.AttemptYielded)
	}
	if err := r.persistLocked(ctx, identity.EffectIdentity.ConfigID, state.revision, state); err != nil {
		return TransitionRejected, err
	}
	r.configs[identity.EffectIdentity.ConfigID.Name] = state
	return TransitionApplied, nil
}

// CompleteEffectObservationAndNode atomically applies a terminal effect
// observation, marks the target plan node terminal, and enqueues the lifecycle
// outbox event in a single execution Revision CAS. This closes the crash window
// where the Effect is terminal but the Node has not advanced.
func (r *PlanRegistry) CompleteEffectObservationAndNode(
	ctx context.Context,
	identity TransitionIdentity,
	observation EffectObservation,
	nodeKey model.OperationKey,
	event model.Event,
) (TransitionDisposition, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.configs[identity.EffectIdentity.ConfigID.Name]
	state := cloneConfigExecution(current)
	if state == nil || state.active == nil {
		return TransitionRejected, errors.New("config not found")
	}

	// --- Validate the observation against the InFlight control ---
	control := state.controls[identity.RequestID]
	if control.ID == "" || control.State != EffectControlInFlight {
		return TransitionStale, nil
	}
	if observation.PollRequestID != "" && observation.PollRequestID != control.PollRequestID {
		return TransitionRejected, errors.New("poll request id mismatch")
	}
	if observation.AttemptID != "" && observation.AttemptID != control.InFlightAttemptID {
		return TransitionRejected, errors.New("attempt id mismatch")
	}
	if identity.AttemptID != "" && identity.AttemptID != control.InFlightAttemptID {
		return TransitionRejected, errors.New("attempt id mismatch")
	}
	if identity.EffectIdentity.EffectID != "" && identity.EffectIdentity.EffectID != control.EffectID {
		return TransitionRejected, errors.New("effect id mismatch")
	}
	if identity.EffectIdentity.ReferenceID != "" && identity.EffectIdentity.ReferenceID != control.ReferenceID {
		return TransitionRejected, errors.New("reference id mismatch")
	}
	effect := state.effects[identity.EffectIdentity.EffectID]
	if effect.ID == "" {
		return TransitionStale, nil
	}
	if observation.EffectID != "" && observation.EffectID != effect.ID {
		return TransitionRejected, errors.New("observation effect id mismatch")
	}
	reference := state.references[identity.EffectIdentity.ReferenceID]
	if reference.ID == "" && observation.Disposition != DispositionAuthoritativeGone {
		return TransitionStale, nil
	}
	oldEffect, oldControl := effect, control

	// --- Apply the terminal observation ---
	switch observation.Disposition {
	case DispositionCompleted, DispositionCancelled:
		control.State = EffectControlCompleted
		control.InFlightAttemptID = ""
		control.PollRequestID = ""
		control.LeaseExpiresAt = time.Time{}
		effect.State = ExternalEffectCompleted
		if observation.Disposition == DispositionCancelled {
			effect.State = ExternalEffectCancelled
		}
		effect.ResolutionRequired = false
		effect.ExternalRevision = observation.ExternalRevision
		if err := ValidateControlTransition(oldControl, control); err != nil {
			return TransitionRejected, err
		}
		if err := ValidateEffectTransition(oldEffect, effect, EffectTransitionExternalObservation); err != nil {
			return TransitionRejected, err
		}
		state.controls[control.ID] = control
		state.effects[effect.ID] = effect

	case DispositionFailed:
		control.State = EffectControlCompleted
		control.InFlightAttemptID = ""
		control.PollRequestID = ""
		control.LeaseExpiresAt = time.Time{}
		effect.State = ExternalEffectFailed
		effect.ResolutionRequired = true
		if observation.ExternalRevision > 0 {
			effect.ExternalRevision = observation.ExternalRevision
		}
		if err := ValidateControlTransition(oldControl, control); err != nil {
			return TransitionRejected, err
		}
		if err := ValidateEffectTransition(oldEffect, effect, EffectTransitionExternalObservation); err != nil {
			return TransitionRejected, err
		}
		state.controls[control.ID] = control
		state.effects[effect.ID] = effect

	case DispositionAuthoritativeGone:
		if effect.Binding != EffectBindingBound {
			return TransitionRejected, errors.New("authoritative gone requires bound effect")
		}
		if observation.ExternalJobID != "" && observation.ExternalJobID != effect.ExternalJobID {
			return TransitionRejected, errors.New("authoritative gone job id mismatch")
		}
		control.State = EffectControlCompleted
		control.InFlightAttemptID = ""
		control.PollRequestID = ""
		control.LeaseExpiresAt = time.Time{}
		if err := ValidateControlTransition(oldControl, control); err != nil {
			return TransitionRejected, err
		}
		state.controls[control.ID] = control
		delete(state.effects, effect.ID)
		for id, ref := range state.references {
			if ref.EffectID != effect.ID {
				continue
			}
			delete(state.references, id)
			for cid, c := range state.controls {
				if c.ReferenceID == id || c.EffectID == effect.ID {
					c.State = EffectControlCompleted
					c.InFlightAttemptID = ""
					c.PollRequestID = ""
					c.LeaseExpiresAt = time.Time{}
					state.controls[cid] = c
				}
			}
		}

	default:
		return TransitionRejected, errors.Errorf("unsupported terminal observation disposition %q", observation.Disposition)
	}

// --- Verify NodeIdentity matches the active plan, preventing
	// cross-generation advancement by a stale control. ---
	if identity.EffectIdentity.PlanID != "" && identity.EffectIdentity.PlanID != state.active.ID {
		return TransitionStale, nil
	}
	if identity.EffectIdentity.Generation != 0 && identity.EffectIdentity.Generation != state.active.Generation {
		return TransitionStale, nil
	}

	// --- Mark the plan node terminal and retire the control attempt ---
	node := state.active.Nodes[nodeKey]
	if node == nil {
		return TransitionRejected, errors.Errorf("operation %q not found", nodeKey)
	}
	attemptID := identity.AttemptID
	if node.Status != model.NodeCompleted {
		attempt := &model.Attempt{
			ID: attemptID, PlanID: state.active.ID, Generation: state.active.Generation,
			ConfigID: identity.EffectIdentity.ConfigID, NodeKey: nodeKey,
			Fingerprint: node.Operation.Fingerprint, ConflictKey: node.Operation.ConflictKey,
			Status: model.AttemptCompleted,
		}
		if event.State == model.StepFailed {
			attempt.Status = model.AttemptFailed
			node.Status = model.NodeFailed
		} else {
			node.Status = model.NodeCompleted
		}
		node.AttemptID = attemptID
		state.retired[attemptID] = attempt
		delete(state.attempts, attemptID)
	}

	// --- Enqueue the lifecycle outbox event in the same CAS ---
	if event.EventID == "" {
		return TransitionRejected, errors.New("outbox event ID is empty")
	}
	state.outbox[event.EventID] = event

	// --- One atomic persist ---
	if err := r.persistLocked(ctx, identity.EffectIdentity.ConfigID, state.revision, state); err != nil {
		return TransitionRejected, err
	}
	r.configs[identity.EffectIdentity.ConfigID.Name] = state
	return TransitionApplied, nil
}

// YieldControl yields an InFlight control with the next check time.
func (r *PlanRegistry) YieldControl(ctx context.Context, identity TransitionIdentity, nextCheckAt time.Time) (TransitionDisposition, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.configs[identity.EffectIdentity.ConfigID.Name]
	state := cloneConfigExecution(current)
	if state == nil {
		return TransitionRejected, nil
	}
	control := state.controls[identity.RequestID]
	if control.ID == "" || control.State != EffectControlInFlight {
		return TransitionStale, nil
	}
	control.State = EffectControlYielded
	control.NextCheckAt = nextCheckAt
	control.InFlightAttemptID = ""
	control.PollRequestID = ""
	control.LeaseExpiresAt = time.Time{}
	state.controls[control.ID] = control
	// Retire the poll attempt as yielded: terminal for the short Attempt,
	// non-terminal for the effect.
	retireControlAttemptLocked(state, identity.AttemptID, model.AttemptYielded)
	if err := r.persistLocked(ctx, identity.EffectIdentity.ConfigID, state.revision, state); err != nil {
		return TransitionRejected, err
	}
	r.configs[identity.EffectIdentity.ConfigID.Name] = state
	return TransitionApplied, nil
}

// ListDueControls returns all controls due at or before the given time.
func (r *PlanRegistry) ListDueControls(_ context.Context, now time.Time) ([]DueControlRef, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var refs []DueControlRef
	for configName, state := range r.configs {
		for _, control := range state.controls {
			if (control.State == EffectControlPending || control.State == EffectControlYielded) && !control.NextCheckAt.After(now) {
				refs = append(refs, DueControlRef{
					ConfigID:         model.ConfigID{Name: configName},
					ControlRequestID: control.ID,
					NextCheckAt:      control.NextCheckAt,
				})
			}
		}
	}
	return refs, nil
}

type DueControlRef struct {
	ConfigID         model.ConfigID
	ControlRequestID ControlRequestID
	NextCheckAt      time.Time
}

// WakeDueControls forces Pending/Yielded controls to be due at or before now.
func (r *PlanRegistry) WakeDueControls(ctx context.Context, now time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for configName, current := range r.configs {
		changed := false
		state := cloneConfigExecution(current)
		for id, control := range state.controls {
			if control.State != EffectControlPending && control.State != EffectControlYielded {
				continue
			}
			if !control.NextCheckAt.After(now) {
				continue
			}
			control.NextCheckAt = now
			state.controls[id] = control
			changed = true
		}
		if !changed {
			continue
		}
		configID := model.ConfigID{Name: configName}
		if err := r.persistLocked(ctx, configID, state.revision, state); err != nil {
			return err
		}
		r.configs[configName] = state
	}
	return nil
}

// ReclaimExpiredControls resets InFlight controls whose leases have expired.
func (r *PlanRegistry) ReclaimExpiredControls(ctx context.Context, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for configName, current := range r.configs {
		var expired []ControlRequestID
		for id, control := range current.controls {
			if control.State == EffectControlInFlight && !control.LeaseExpiresAt.IsZero() && !control.LeaseExpiresAt.After(now) {
				expired = append(expired, id)
			}
		}
		if len(expired) == 0 {
			continue
		}
		state := cloneConfigExecution(current)
		for _, id := range expired {
			control := state.controls[id]
			oldControl := control
			retireControlAttemptLocked(state, control.InFlightAttemptID, model.AttemptUnknown)
			control.State = EffectControlPending
			control.InFlightAttemptID = ""
			control.PollRequestID = ""
			control.LeaseExpiresAt = time.Time{}
			control.RetryCount++
			control.NextCheckAt = now
			if err := ValidateControlTransition(oldControl, control); err != nil {
				continue
			}
			state.controls[id] = control
		}
		configID := model.ConfigID{Name: configName}
		if err := r.persistLocked(ctx, configID, state.revision, state); err != nil {
			continue
		}
		r.configs[configName] = state
	}
}

// ReclaimExpiredControl resets one expired InFlight control.
func (r *PlanRegistry) ReclaimExpiredControl(ctx context.Context, configID model.ConfigID, controlID ControlRequestID, now time.Time) (TransitionDisposition, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.configs[configID.Name]
	state := cloneConfigExecution(current)
	if state == nil {
		return TransitionRejected, errors.New("config not found")
	}
	control := state.controls[controlID]
	if control.ID == "" {
		return TransitionStale, nil
	}
	if control.State != EffectControlInFlight {
		return TransitionDuplicate, nil
	}
	if control.LeaseExpiresAt.IsZero() || control.LeaseExpiresAt.After(now) {
		return TransitionRejected, errors.New("control lease not expired")
	}
	oldControl := control
	retireControlAttemptLocked(state, control.InFlightAttemptID, model.AttemptUnknown)
	control.State = EffectControlPending
	control.InFlightAttemptID = ""
	control.PollRequestID = ""
	control.LeaseExpiresAt = time.Time{}
	control.RetryCount++
	control.NextCheckAt = now
	if err := ValidateControlTransition(oldControl, control); err != nil {
		return TransitionRejected, err
	}
	state.controls[control.ID] = control
	if err := r.persistLocked(ctx, configID, state.revision, state); err != nil {
		return TransitionRejected, err
	}
	r.configs[configID.Name] = state
	return TransitionApplied, nil
}

// BeginReleaseEffect marks a reference ReleaseRequested and schedules a Release control.
func (r *PlanRegistry) BeginReleaseEffect(ctx context.Context, req BeginReleaseRequest) (TransitionDisposition, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.configs[req.Identity.EffectIdentity.ConfigID.Name]
	state := cloneConfigExecution(current)
	if state == nil {
		return TransitionRejected, errors.New("config not found")
	}
	reference := state.references[req.Identity.EffectIdentity.ReferenceID]
	if reference.ID == "" {
		return TransitionStale, nil
	}
	if reference.State == EffectReferenceReleaseRequested || reference.State == EffectReferenceReleased {
		return TransitionDuplicate, nil
	}
	effect := state.effects[reference.EffectID]
	if effect.ID == "" {
		return TransitionStale, nil
	}
	oldReference := reference
	reference.State = EffectReferenceReleaseRequested
	if err := ValidateReferenceTransition(oldReference, reference); err != nil {
		return TransitionRejected, err
	}
	state.references[reference.ID] = reference
	retireIncompatibleControlsLocked(state, reference.ID)
	controlID := req.Identity.RequestID
	if controlID == "" {
		controlID = ControlRequestID("release-" + string(reference.ID))
	}
	if _, exists := state.controls[controlID]; !exists {
		state.controls[controlID] = EffectControl{
			ID: controlID, ConfigID: reference.ConfigID,
			ProviderType: effect.ProviderType, ProviderDigest: effect.ProviderDigest,
			Kind: EffectControlRelease, State: EffectControlPending,
			EffectID: effect.ID, ReferenceID: reference.ID,
			PlanID: req.Identity.EffectIdentity.PlanID, Generation: req.Identity.EffectIdentity.Generation,
			OperationKey: req.Identity.EffectIdentity.OperationKey,
			NextCheckAt:  time.Now(),
		}
	}
	if err := r.persistLocked(ctx, reference.ConfigID, state.revision, state); err != nil {
		return TransitionRejected, err
	}
	r.configs[reference.ConfigID.Name] = state
	return TransitionApplied, nil
}

// ApplyReleaseResult applies a provider release disposition to reference/effect state.
func (r *PlanRegistry) ApplyReleaseResult(ctx context.Context, identity TransitionIdentity, result ReleaseEffectResult) (TransitionDisposition, error) {
	if err := ValidateReleaseDispositionFailure(result.Disposition, result.Failure); err != nil {
		return TransitionRejected, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.configs[identity.EffectIdentity.ConfigID.Name]
	state := cloneConfigExecution(current)
	if state == nil {
		return TransitionRejected, nil
	}
	reference := state.references[identity.EffectIdentity.ReferenceID]
	if reference.ID == "" {
		return TransitionStale, nil
	}
	effect := state.effects[identity.EffectIdentity.EffectID]
	if effect.ID == "" {
		return TransitionStale, nil
	}
	control := state.controls[identity.RequestID]
	switch result.Disposition {
	case ReleaseStillReferenced, ReleaseConfirmed:
		reference.State = EffectReferenceReleased
		state.references[reference.ID] = reference
		if control.ID != "" {
			control.State = EffectControlCompleted
			control.InFlightAttemptID = ""
			control.PollRequestID = ""
			control.LeaseExpiresAt = time.Time{}
			state.controls[control.ID] = control
		}
		if result.Disposition == ReleaseConfirmed {
			effect.State = ExternalEffectCompleted
			effect.ResolutionRequired = false
			state.effects[effect.ID] = effect
		}
	case ReleaseLastReferenceCancelRequested:
		reference.State = EffectReferenceReleased
		state.references[reference.ID] = reference
		effect.State = ExternalEffectCancelling
		effect.ResolutionRequired = true
		state.effects[effect.ID] = effect
		if control.ID != "" {
			control.State = EffectControlCompleted
			control.InFlightAttemptID = ""
			control.PollRequestID = ""
			control.LeaseExpiresAt = time.Time{}
			state.controls[control.ID] = control
		}
		cancelControlID := ControlRequestID("observe-cancel-" + string(effect.ID))
		if _, exists := state.controls[cancelControlID]; !exists {
			state.controls[cancelControlID] = EffectControl{
				ID: cancelControlID, ConfigID: identity.EffectIdentity.ConfigID,
				ProviderType: effect.ProviderType, ProviderDigest: effect.ProviderDigest,
				Kind: EffectControlObserveCancellation, State: EffectControlPending,
				EffectID: effect.ID, ReferenceID: reference.ID, NextCheckAt: time.Now(),
			}
		}
	case ReleaseUnknown:
		if control.ID != "" {
			control.State = EffectControlYielded
			control.NextCheckAt = time.Now().Add(5 * time.Second)
			control.InFlightAttemptID = ""
			control.PollRequestID = ""
			control.LeaseExpiresAt = time.Time{}
			state.controls[control.ID] = control
		}
		effect.ResolutionRequired = true
		state.effects[effect.ID] = effect
	default:
		if result.Failure == ReleaseFailurePermanent {
			effect.State = ExternalEffectFailed
			effect.ResolutionRequired = true
			state.effects[effect.ID] = effect
		}
		if control.ID != "" {
			control.State = EffectControlYielded
			control.NextCheckAt = time.Now().Add(5 * time.Second)
			control.InFlightAttemptID = ""
			control.PollRequestID = ""
			control.LeaseExpiresAt = time.Time{}
			state.controls[control.ID] = control
		}
	}
	// Retire the control-poll attempt: terminal release dispositions complete,
	// unknown/transient yield for retry.
	switch result.Disposition {
	case ReleaseUnknown:
		retireControlAttemptLocked(state, identity.AttemptID, model.AttemptYielded)
	default:
		if result.Failure == ReleaseFailurePermanent {
			retireControlAttemptLocked(state, identity.AttemptID, model.AttemptFailed)
		} else {
			retireControlAttemptLocked(state, identity.AttemptID, model.AttemptCompleted)
		}
	}
	if err := r.persistLocked(ctx, identity.EffectIdentity.ConfigID, state.revision, state); err != nil {
		return TransitionRejected, err
	}
	r.configs[identity.EffectIdentity.ConfigID.Name] = state
	return TransitionApplied, nil
}

// CompleteReleaseAndNode atomically applies a terminal release disposition,
// marks the target plan node terminal, and enqueues the lifecycle outbox event
// in a single execution Revision CAS. It closes the crash window where the
// reference is Released but the release node has not advanced.
func (r *PlanRegistry) CompleteReleaseAndNode(
	ctx context.Context,
	identity TransitionIdentity,
	result ReleaseEffectResult,
	nodeKey model.OperationKey,
	event model.Event,
) (TransitionDisposition, error) {
	if err := ValidateReleaseDispositionFailure(result.Disposition, result.Failure); err != nil {
		return TransitionRejected, err
	}
	switch result.Disposition {
	case ReleaseStillReferenced, ReleaseConfirmed, ReleaseLastReferenceCancelRequested:
	default:
		return TransitionRejected, errors.Errorf("unsupported terminal release disposition %q", result.Disposition)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.configs[identity.EffectIdentity.ConfigID.Name]
	state := cloneConfigExecution(current)
	if state == nil || state.active == nil {
		return TransitionRejected, errors.New("config not found")
	}
	reference := state.references[identity.EffectIdentity.ReferenceID]
	if reference.ID == "" {
		return TransitionStale, nil
	}
	effect := state.effects[identity.EffectIdentity.EffectID]
	if effect.ID == "" {
		return TransitionStale, nil
	}
	control := state.controls[identity.RequestID]
	oldReference := reference
	var oldControl EffectControl
	hasControl := control.ID != ""
	if hasControl {
		oldControl = control
	}

	reference.State = EffectReferenceReleased
	if err := ValidateReferenceTransition(oldReference, reference); err != nil {
		return TransitionRejected, err
	}
	state.references[reference.ID] = reference

	switch result.Disposition {
	case ReleaseConfirmed:
		effect.State = ExternalEffectCompleted
		effect.ResolutionRequired = false
	case ReleaseLastReferenceCancelRequested:
		effect.State = ExternalEffectCancelling
		effect.ResolutionRequired = true
	}
	state.effects[effect.ID] = effect

	if hasControl {
		control.State = EffectControlCompleted
		control.InFlightAttemptID = ""
		control.PollRequestID = ""
		control.LeaseExpiresAt = time.Time{}
		if oldControl.State == EffectControlInFlight {
			if err := ValidateControlTransition(oldControl, control); err != nil {
				return TransitionRejected, err
			}
		}
		state.controls[control.ID] = control
	}
	if result.Disposition == ReleaseLastReferenceCancelRequested {
		cancelControlID := ControlRequestID("observe-cancel-" + string(effect.ID))
		if _, exists := state.controls[cancelControlID]; !exists {
			state.controls[cancelControlID] = EffectControl{
				ID: cancelControlID, ConfigID: identity.EffectIdentity.ConfigID,
				ProviderType: effect.ProviderType, ProviderDigest: effect.ProviderDigest,
				Kind: EffectControlObserveCancellation, State: EffectControlPending,
				EffectID: effect.ID, ReferenceID: reference.ID, NextCheckAt: time.Now(),
			}
		}
// --- Verify NodeIdentity matches the active plan, preventing
	// cross-generation advancement by a stale control. ---
	if identity.EffectIdentity.PlanID != "" && identity.EffectIdentity.PlanID != state.active.ID {
		return TransitionStale, nil
	}
	if identity.EffectIdentity.Generation != 0 && identity.EffectIdentity.Generation != state.active.Generation {
		return TransitionStale, nil
	}	}

	// --- Mark the plan node terminal and retire the control attempt ---
	node := state.active.Nodes[nodeKey]
	if node == nil {
		return TransitionRejected, errors.Errorf("operation %q not found", nodeKey)
	}
	if node.Status != model.NodeCompleted {
		attempt := &model.Attempt{
			ID: identity.AttemptID, PlanID: state.active.ID, Generation: state.active.Generation,
			ConfigID: identity.EffectIdentity.ConfigID, NodeKey: nodeKey,
			Fingerprint: node.Operation.Fingerprint, ConflictKey: node.Operation.ConflictKey,
			Status: model.AttemptCompleted,
		}
		node.Status = model.NodeCompleted
		node.AttemptID = identity.AttemptID
		state.retired[identity.AttemptID] = attempt
		delete(state.attempts, identity.AttemptID)
	}

	// --- Enqueue the lifecycle outbox event in the same CAS ---
	if event.EventID == "" {
		return TransitionRejected, errors.New("outbox event ID is empty")
	}
	state.outbox[event.EventID] = event

	if err := r.persistLocked(ctx, identity.EffectIdentity.ConfigID, state.revision, state); err != nil {
		return TransitionRejected, err
	}
	r.configs[identity.EffectIdentity.ConfigID.Name] = state
	return TransitionApplied, nil
}


// BeginEnsureReference persists a new Ensuring reference against an already-bound effect.
func (r *PlanRegistry) BeginEnsureReference(ctx context.Context, identity TransitionIdentity) (TransitionDisposition, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.configs[identity.EffectIdentity.ConfigID.Name]
	state := cloneConfigExecution(current)
	if state == nil {
		return TransitionRejected, errors.New("config not found")
	}
	effect := state.effects[identity.EffectIdentity.EffectID]
	if effect.ID == "" || effect.Binding != EffectBindingBound {
		return TransitionRejected, errors.New("effect is not bound")
	}
	if _, exists := state.references[identity.EffectIdentity.ReferenceID]; exists {
		return TransitionDuplicate, nil
	}
	reference := EffectReference{
		ID: identity.EffectIdentity.ReferenceID, EffectID: effect.ID,
		ConfigID: identity.EffectIdentity.ConfigID, PlanID: identity.EffectIdentity.PlanID,
		Generation: identity.EffectIdentity.Generation, EffectKey: identity.EffectIdentity.EffectKey,
		State: EffectReferenceEnsuring,
	}
	controlID := identity.RequestID
	if controlID == "" {
		controlID = ControlRequestID("ensure-ref-" + string(reference.ID))
	}
	state.references[reference.ID] = reference
	state.controls[controlID] = EffectControl{
		ID: controlID, ConfigID: reference.ConfigID,
		ProviderType: effect.ProviderType, ProviderDigest: effect.ProviderDigest,
		Kind: EffectControlEnsureReference, State: EffectControlPending,
		EffectID: effect.ID, ReferenceID: reference.ID, NextCheckAt: time.Now(),
	}
	if err := r.persistLocked(ctx, reference.ConfigID, state.revision, state); err != nil {
		return TransitionRejected, err
	}
	r.configs[reference.ConfigID.Name] = state
	return TransitionApplied, nil
}

// ApplyEnsureReferenceResult activates a reference after the service confirms it.
func (r *PlanRegistry) ApplyEnsureReferenceResult(ctx context.Context, identity TransitionIdentity, result EnsureReferenceResult) (TransitionDisposition, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.configs[identity.EffectIdentity.ConfigID.Name]
	state := cloneConfigExecution(current)
	if state == nil {
		return TransitionRejected, nil
	}
	reference := state.references[identity.EffectIdentity.ReferenceID]
	if reference.ID == "" {
		return TransitionStale, nil
	}
	if reference.State == EffectReferenceActive {
		return TransitionDuplicate, nil
	}
	if result.Disposition != EnsureBound {
		return TransitionRejected, errors.Errorf("ensure reference disposition %q", result.Disposition)
	}
	oldReference := reference
	reference.State = EffectReferenceActive
	if err := ValidateReferenceTransition(oldReference, reference); err != nil {
		return TransitionRejected, err
	}
	state.references[reference.ID] = reference
	if control, ok := state.controls[identity.RequestID]; ok {
		oldControl := control
		control.State = EffectControlCompleted
		control.InFlightAttemptID = ""
		control.PollRequestID = ""
		control.LeaseExpiresAt = time.Time{}
		if oldControl.State == EffectControlInFlight {
			if err := ValidateControlTransition(oldControl, control); err != nil {
				return TransitionRejected, err
			}
		}
		state.controls[control.ID] = control
	}
	// After the replacement reference is Active, schedule release of older
	// Active/ReleaseRequested refs for the same EffectKey on this config.
	for id, old := range state.references {
		if old.EffectKey != reference.EffectKey || old.ID == reference.ID {
			continue
		}
		if old.State != EffectReferenceActive && old.State != EffectReferenceEnsuring {
			continue
		}
		if old.Generation >= reference.Generation {
			continue
		}
		old.State = EffectReferenceReleaseRequested
		state.references[id] = old
		retireIncompatibleControlsLocked(state, old.ID)
		releaseID := ControlRequestID("release-" + string(old.ID))
		if _, exists := state.controls[releaseID]; !exists {
			effect := state.effects[old.EffectID]
			state.controls[releaseID] = EffectControl{
				ID: releaseID, ConfigID: old.ConfigID,
				ProviderType: effect.ProviderType, ProviderDigest: effect.ProviderDigest,
				Kind: EffectControlRelease, State: EffectControlPending,
				EffectID: old.EffectID, ReferenceID: old.ID, NextCheckAt: time.Now(),
			}
		}
	}
	retireControlAttemptLocked(state, identity.AttemptID, model.AttemptCompleted)
	if err := r.persistLocked(ctx, identity.EffectIdentity.ConfigID, state.revision, state); err != nil {
		return TransitionRejected, err
	}
	r.configs[identity.EffectIdentity.ConfigID.Name] = state
	return TransitionApplied, nil
}


// CompleteEnsureReferenceAndNode atomically activates an EnsureReference result,
// marks the target ensure node terminal, and enqueues the lifecycle outbox event
// in a single execution Revision CAS. It closes the crash window where the
// reference is Active but the ensure node has not advanced.
func (r *PlanRegistry) CompleteEnsureReferenceAndNode(
	ctx context.Context,
	identity TransitionIdentity,
	result EnsureReferenceResult,
	nodeKey model.OperationKey,
	event model.Event,
) (TransitionDisposition, error) {
	if result.Disposition != EnsureBound {
		return TransitionRejected, errors.Errorf("ensure reference disposition %q", result.Disposition)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.configs[identity.EffectIdentity.ConfigID.Name]
	state := cloneConfigExecution(current)
	if state == nil || state.active == nil {
		return TransitionRejected, errors.New("config not found")
	}
	reference := state.references[identity.EffectIdentity.ReferenceID]
	if reference.ID == "" {
		return TransitionStale, nil
	}
	if reference.State == EffectReferenceActive {
		return TransitionDuplicate, nil
	}
	oldReference := reference
	reference.State = EffectReferenceActive
	if err := ValidateReferenceTransition(oldReference, reference); err != nil {
		return TransitionRejected, err
	}
	state.references[reference.ID] = reference
	if control, ok := state.controls[identity.RequestID]; ok {
		oldControl := control
		control.State = EffectControlCompleted
		control.InFlightAttemptID = ""
		control.PollRequestID = ""
		control.LeaseExpiresAt = time.Time{}
		if oldControl.State == EffectControlInFlight {
			if err := ValidateControlTransition(oldControl, control); err != nil {
				return TransitionRejected, err
			}
		}
		state.controls[control.ID] = control
	}
	// After the replacement reference is Active, schedule release of older
	// Active/ReleaseRequested refs for the same EffectKey on this config.
	for id, old := range state.references {
		if old.EffectKey != reference.EffectKey || old.ID == reference.ID {
			continue
		}
		if old.State != EffectReferenceActive && old.State != EffectReferenceEnsuring {
			continue
		}
		if old.Generation >= reference.Generation {
			continue
		}
		old.State = EffectReferenceReleaseRequested
		state.references[id] = old
		retireIncompatibleControlsLocked(state, old.ID)
		releaseID := ControlRequestID("release-" + string(old.ID))
		if _, exists := state.controls[releaseID]; !exists {
			effect := state.effects[old.EffectID]
			state.controls[releaseID] = EffectControl{
				ID: releaseID, ConfigID: old.ConfigID,
				ProviderType: effect.ProviderType, ProviderDigest: effect.ProviderDigest,
				Kind: EffectControlRelease, State: EffectControlPending,
				EffectID: old.EffectID, ReferenceID: old.ID, NextCheckAt: time.Now(),
			}
		}
	}

// --- Verify NodeIdentity matches the active plan, preventing
	// cross-generation advancement by a stale control. ---
	if identity.EffectIdentity.PlanID != "" && identity.EffectIdentity.PlanID != state.active.ID {
		return TransitionStale, nil
	}
	if identity.EffectIdentity.Generation != 0 && identity.EffectIdentity.Generation != state.active.Generation {
		return TransitionStale, nil
	}	// --- Mark the plan node terminal and retire the control attempt ---
	node := state.active.Nodes[nodeKey]
	if node == nil {
		return TransitionRejected, errors.Errorf("operation %q not found", nodeKey)
	}
	if node.Status != model.NodeCompleted {
		attempt := &model.Attempt{
			ID: identity.AttemptID, PlanID: state.active.ID, Generation: state.active.Generation,
			ConfigID: identity.EffectIdentity.ConfigID, NodeKey: nodeKey,
			Fingerprint: node.Operation.Fingerprint, ConflictKey: node.Operation.ConflictKey,
			Status: model.AttemptCompleted,
		}
		node.Status = model.NodeCompleted
		node.AttemptID = identity.AttemptID
		state.retired[identity.AttemptID] = attempt
		delete(state.attempts, identity.AttemptID)
	}

	// --- Enqueue the lifecycle outbox event in the same CAS ---
	if event.EventID == "" {
		return TransitionRejected, errors.New("outbox event ID is empty")
	}
	state.outbox[event.EventID] = event

	if err := r.persistLocked(ctx, identity.EffectIdentity.ConfigID, state.revision, state); err != nil {
		return TransitionRejected, err
	}
	r.configs[identity.EffectIdentity.ConfigID.Name] = state
	return TransitionApplied, nil
}

// LookupEffectAndReference returns durable effect+reference by IDs.
func (r *PlanRegistry) LookupEffectAndReference(configID model.ConfigID, effectID EffectID, referenceID ReferenceID) (ActiveEffect, EffectReference, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	state := r.configs[configID.Name]
	if state == nil {
		return ActiveEffect{}, EffectReference{}, false
	}
	effect, ok := state.effects[effectID]
	if !ok {
		return ActiveEffect{}, EffectReference{}, false
	}
	reference, ok := state.references[referenceID]
	if !ok {
		return ActiveEffect{}, EffectReference{}, false
	}
	return effect, reference, true
}


// HasActiveEffectControl reports whether a non-terminal control owns an effect slot.
func (r *PlanRegistry) HasActiveEffectControl(configID model.ConfigID, effectKey string, kind EffectControlKind) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	state := r.configs[configID.Name]
	if state == nil {
		return false
	}
	for _, control := range state.controls {
		if control.Kind != kind || control.State == EffectControlCompleted {
			continue
		}
		ref := state.references[control.ReferenceID]
		if ref.EffectKey == effectKey {
			return true
		}
	}
	return false
}

// MarkEffectUnknownBound records transport-unknown after a Bound effect poll/release
// and reschedules Observe or ObserveCancellation.
func (r *PlanRegistry) MarkEffectUnknownBound(ctx context.Context, identity TransitionIdentity, nextCheckAt time.Time) (TransitionDisposition, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.configs[identity.EffectIdentity.ConfigID.Name]
	state := cloneConfigExecution(current)
	if state == nil {
		return TransitionRejected, nil
	}
	effect := state.effects[identity.EffectIdentity.EffectID]
	if effect.ID == "" || effect.Binding != EffectBindingBound {
		return TransitionStale, nil
	}
	oldEffect := effect
	effect.State = ExternalEffectUnknown
	effect.ResolutionRequired = true
	if err := ValidateEffectTransition(oldEffect, effect, EffectTransitionCoreResolution); err != nil {
		return TransitionRejected, err
	}
	state.effects[effect.ID] = effect
	control := state.controls[identity.RequestID]
	if control.ID != "" {
		oldControl := control
		if nextCheckAt.IsZero() {
			nextCheckAt = time.Now().Add(5 * time.Second)
		}
		control.State = EffectControlYielded
		control.NextCheckAt = nextCheckAt
		control.InFlightAttemptID = ""
		control.PollRequestID = ""
		control.LeaseExpiresAt = time.Time{}
		if oldControl.State == EffectControlInFlight {
			if err := ValidateControlTransition(oldControl, control); err != nil {
				return TransitionRejected, err
			}
		}
		state.controls[control.ID] = control
	}
	retireControlAttemptLocked(state, identity.AttemptID, model.AttemptYielded)
	if err := r.persistLocked(ctx, identity.EffectIdentity.ConfigID, state.revision, state); err != nil {
		return TransitionRejected, err
	}
	r.configs[identity.EffectIdentity.ConfigID.Name] = state
	return TransitionApplied, nil
}

// AdministratorResolveFailedEffect clears fail-closed ResolutionRequired on a
// Failed effect after an audited administrator decision.
func (r *PlanRegistry) AdministratorResolveFailedEffect(ctx context.Context, configID model.ConfigID, effectID EffectID, auditReason string) (TransitionDisposition, error) {
	if auditReason == "" {
		return TransitionRejected, errors.New("administrator resolve requires audit reason")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.configs[configID.Name]
	state := cloneConfigExecution(current)
	if state == nil {
		return TransitionRejected, errors.New("config not found")
	}
	effect := state.effects[effectID]
	if effect.ID == "" {
		return TransitionStale, nil
	}
	if effect.State != ExternalEffectFailed || !effect.ResolutionRequired {
		return TransitionRejected, errors.New("administrator resolve applies only to failed resolution-required effects")
	}
	effect.ResolutionRequired = false
	state.effects[effect.ID] = effect
	if err := r.persistLocked(ctx, configID, state.revision, state); err != nil {
		return TransitionRejected, err
	}
	r.configs[configID.Name] = state
	return TransitionApplied, nil
}

// RegistryCommands is the compile-checked durable effect command surface.
type RegistryCommands interface {
	BeginEnsureEffect(context.Context, BeginEnsureRequest) (TransitionDisposition, error)
	ApplyEnsureResult(context.Context, TransitionIdentity, EnsureEffectResult) (TransitionDisposition, error)
	BeginEnsureReference(context.Context, TransitionIdentity) (TransitionDisposition, error)
	ApplyEnsureReferenceResult(context.Context, TransitionIdentity, EnsureReferenceResult) (TransitionDisposition, error)
	ApplyEffectObservation(context.Context, TransitionIdentity, EffectObservation) (TransitionDisposition, error)
	BeginReleaseEffect(context.Context, BeginReleaseRequest) (TransitionDisposition, error)
	ApplyReleaseResult(context.Context, TransitionIdentity, ReleaseEffectResult) (TransitionDisposition, error)
	ListDueControls(context.Context, time.Time) ([]DueControlRef, error)
	ClaimDueControl(context.Context, model.ConfigID, ControlRequestID, time.Time, model.AttemptID, PollRequestID, time.Time) (*EffectControl, error)
	ReclaimExpiredControl(context.Context, model.ConfigID, ControlRequestID, time.Time) (TransitionDisposition, error)
	YieldControl(context.Context, TransitionIdentity, time.Time) (TransitionDisposition, error)
}

var _ RegistryCommands = (*PlanRegistry)(nil)

// EnsureObserveControl creates an Observe control for a bound effect if one does
// not already exist. It is called by the DAG observe node to hand polling
// ownership to the EffectControl scheduler.

// EnsureEnsureRetryControl creates an EnsureRetry control for an effect if one
// does not already exist, bound to the ensure node. The DAG ensure path calls
// this to hand EnsureEffect RPC ownership to the scheduler.
func (r *PlanRegistry) EnsureEnsureRetryControl(ctx context.Context, configID model.ConfigID, effectID EffectID, referenceID ReferenceID, planID model.PlanID, generation model.Generation, opKey model.OperationKey) (TransitionDisposition, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.configs[configID.Name]
	state := cloneConfigExecution(current)
	if state == nil {
		return TransitionRejected, errors.New("config not found")
	}
	effect := state.effects[effectID]
	if effect.ID == "" {
		return TransitionStale, nil
	}
	ctrlID := ControlRequestID("ensure-" + string(effectID))
	if _, exists := state.controls[ctrlID]; exists {
		return TransitionDuplicate, nil
	}
	control := EffectControl{
		ID: ctrlID, ConfigID: configID,
		ProviderType: effect.ProviderType, ProviderDigest: effect.ProviderDigest,
		Kind: EffectControlEnsureRetry, State: EffectControlPending,
		EffectID: effect.ID, ReferenceID: referenceID,
		PlanID: planID, Generation: generation, OperationKey: opKey,
		NextCheckAt: time.Now(),
	}
	state.controls[control.ID] = control
	if err := r.persistLocked(ctx, configID, state.revision, state); err != nil {
		return TransitionRejected, err
	}
	r.configs[configID.Name] = state
	return TransitionApplied, nil
}


// RetireControlAttempt retires a claimed control-poll Attempt with the given
// terminal status. It is used by the scheduler for non-node-completing control
// outcomes (e.g., EnsureUnknown retry, EnsureFailed) where the DAG node is not
// advanced by a completion command.
func (r *PlanRegistry) RetireControlAttempt(ctx context.Context, identity TransitionIdentity, status model.AttemptStatus) (TransitionDisposition, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.configs[identity.EffectIdentity.ConfigID.Name]
	state := cloneConfigExecution(current)
	if state == nil {
		return TransitionRejected, errors.New("config not found")
	}
	retireControlAttemptLocked(state, identity.AttemptID, status)
	if err := r.persistLocked(ctx, identity.EffectIdentity.ConfigID, state.revision, state); err != nil {
		return TransitionRejected, err
	}
	r.configs[identity.EffectIdentity.ConfigID.Name] = state
	return TransitionApplied, nil
}

func (r *PlanRegistry) EnsureObserveControl(ctx context.Context, configID model.ConfigID, effectID EffectID, referenceID ReferenceID, planID model.PlanID, generation model.Generation, opKey model.OperationKey) (TransitionDisposition, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.configs[configID.Name]
	state := cloneConfigExecution(current)
	if state == nil {
		return TransitionRejected, errors.New("config not found")
	}
	effect := state.effects[effectID]
	if effect.ID == "" {
		return TransitionStale, nil
	}
	ctrlID := ControlRequestID("observe-" + string(referenceID))
	if _, exists := state.controls[ctrlID]; exists {
		return TransitionDuplicate, nil
	}
	control := EffectControl{
		ID: ctrlID, ConfigID: configID,
		ProviderType: effect.ProviderType, ProviderDigest: effect.ProviderDigest,
		Kind: EffectControlObserve, State: EffectControlPending,
		EffectID: effect.ID, ReferenceID: referenceID,
		PlanID: planID, Generation: generation, OperationKey: opKey,
		NextCheckAt: time.Now(),
	}
	state.controls[control.ID] = control
	if err := r.persistLocked(ctx, configID, state.revision, state); err != nil {
		return TransitionRejected, err
	}
	r.configs[configID.Name] = state
	return TransitionApplied, nil
}
