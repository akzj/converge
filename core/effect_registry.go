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
	if _, exists := state.effects[req.Identity.EffectIdentity.EffectID]; exists {
		return TransitionDuplicate, nil
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
		NextCheckAt: time.Now(),
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

// ApplyEnsureResult transitions an Ensuring effect to Active with its external
// JobID and revision. The effect is bound, the reference becomes Active, and
// an Observe control replaces the EnsureRetry control.
func (r *PlanRegistry) ApplyEnsureResult(ctx context.Context, identity TransitionIdentity, result EnsureEffectResult) (TransitionDisposition, error) {
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
	if effect.State != ExternalEffectEnsuring {
		return TransitionDuplicate, nil
	}
	oldEffect := effect
	oldReference := state.references[identity.EffectIdentity.ReferenceID]
	effect.Binding = EffectBindingBound
	effect.ExternalJobID = result.ExternalJobID
	effect.ExternalRevision = result.ExternalRevision
	effect.State = ExternalEffectActive
	effect.ResolutionRequired = true
	if err := ValidateEffectTransition(oldEffect, effect, EffectTransitionEnsureResult); err != nil {
		return TransitionRejected, err
	}
	state.effects[effect.ID] = effect

	reference := state.references[identity.EffectIdentity.ReferenceID]
	reference.State = EffectReferenceActive
	if oldReference.ID != "" {
		if err := ValidateReferenceTransition(oldReference, reference); err != nil {
			return TransitionRejected, err
		}
	}
	state.references[reference.ID] = reference

	observeControl := EffectControl{
		ID:       ControlRequestID("observe-" + string(identity.EffectIdentity.EffectID)),
		ConfigID: identity.EffectIdentity.ConfigID, ProviderType: effect.ProviderType,
		ProviderDigest: effect.ProviderDigest, Kind: EffectControlObserve,
		State: EffectControlPending, EffectID: effect.ID, ReferenceID: identity.EffectIdentity.ReferenceID,
		NextCheckAt: time.Now(),
	}
	delete(state.controls, identity.RequestID)
	state.controls[observeControl.ID] = observeControl

	if err := r.persistLocked(ctx, identity.EffectIdentity.ConfigID, state.revision, state); err != nil {
		return TransitionRejected, err
	}
	r.configs[identity.EffectIdentity.ConfigID.Name] = state
	return TransitionApplied, nil
}

// ClaimDueControl atomically transitions a Pending/Yielded control to InFlight
// with the given AttemptID, PollRequestID, and lease expiration.
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
	effect := state.effects[identity.EffectIdentity.EffectID]
	if effect.ID == "" {
		return TransitionStale, nil
	}
	reference := state.references[identity.EffectIdentity.ReferenceID]
	if reference.ID == "" {
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

	default:
		return TransitionRejected, errors.Errorf("unhandled observation disposition %q", observation.Disposition)
	}
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
	if err := r.persistLocked(ctx, identity.EffectIdentity.ConfigID, state.revision, state); err != nil {
		return TransitionRejected, err
	}
	r.configs[identity.EffectIdentity.ConfigID.Name] = state
	return TransitionApplied, nil
}

// ListDueControls returns all controls due at or before the given time.
func (r *PlanRegistry) ListDueControls(now time.Time) []DueControlRef {
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
	return refs
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
			EffectID: effect.ID, ReferenceID: reference.ID, NextCheckAt: time.Now(),
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

// FindEffectOperation finds a plan operation by effect key and preferred kind.
func (r *PlanRegistry) FindEffectOperation(configID model.ConfigID, effectKey string, kind model.OperationExecutionKind) (*model.Plan, model.Operation, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	state := r.configs[configID.Name]
	if state == nil || state.active == nil {
		return nil, model.Operation{}, false
	}
	var fallback model.Operation
	var foundFallback bool
	for _, node := range state.active.Nodes {
		op := node.Operation
		if op.EffectKey != effectKey {
			continue
		}
		if op.ExecutionKind == kind {
			return state.active.Clone(), op, true
		}
		if op.ExecutionKind == model.ExecutionEffectRelease || op.ExecutionKind == model.ExecutionEffectObserve {
			fallback = op
			foundFallback = true
		}
	}
	if foundFallback {
		return state.active.Clone(), fallback, true
	}
	return nil, model.Operation{}, false
}

// CompleteEffectOperation marks a plan effect node completed using a control attempt.
func (r *PlanRegistry) CompleteEffectOperation(ctx context.Context, identity TransitionIdentity, key model.OperationKey, state model.StepState) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	current := r.configs[identity.EffectIdentity.ConfigID.Name]
	exec := cloneConfigExecution(current)
	if exec == nil || exec.active == nil {
		return errors.New("config not found")
	}
	node := exec.active.Nodes[key]
	if node == nil {
		return errors.Errorf("operation %q not found", key)
	}
	if node.Status == model.NodeCompleted {
		return nil
	}
	attemptID := identity.AttemptID
	attempt := &model.Attempt{
		ID: attemptID, PlanID: exec.active.ID, Generation: exec.active.Generation,
		ConfigID: identity.EffectIdentity.ConfigID, NodeKey: key,
		Fingerprint: node.Operation.Fingerprint, ConflictKey: node.Operation.ConflictKey,
		Status: model.AttemptCompleted,
	}
	if state == model.StepFailed {
		attempt.Status = model.AttemptFailed
		node.Status = model.NodeFailed
	} else {
		node.Status = model.NodeCompleted
	}
	node.AttemptID = attemptID
	exec.retired[attemptID] = attempt
	delete(exec.attempts, attemptID)
	if err := r.persistLocked(ctx, identity.EffectIdentity.ConfigID, exec.revision, exec); err != nil {
		return err
	}
	r.configs[identity.EffectIdentity.ConfigID.Name] = exec
	return nil
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

// RegistryCommands is the compile-checked durable effect command surface.
type RegistryCommands interface {
	BeginEnsureEffect(context.Context, BeginEnsureRequest) (TransitionDisposition, error)
	ApplyEnsureResult(context.Context, TransitionIdentity, EnsureEffectResult) (TransitionDisposition, error)
	BeginEnsureReference(context.Context, TransitionIdentity) (TransitionDisposition, error)
	ApplyEnsureReferenceResult(context.Context, TransitionIdentity, EnsureReferenceResult) (TransitionDisposition, error)
	ApplyEffectObservation(context.Context, TransitionIdentity, EffectObservation) (TransitionDisposition, error)
	BeginReleaseEffect(context.Context, BeginReleaseRequest) (TransitionDisposition, error)
	ApplyReleaseResult(context.Context, TransitionIdentity, ReleaseEffectResult) (TransitionDisposition, error)
	ListDueControls(time.Time) []DueControlRef
	ClaimDueControl(context.Context, model.ConfigID, ControlRequestID, time.Time, model.AttemptID, PollRequestID, time.Time) (*EffectControl, error)
	ReclaimExpiredControl(context.Context, model.ConfigID, ControlRequestID, time.Time) (TransitionDisposition, error)
	YieldControl(context.Context, TransitionIdentity, time.Time) (TransitionDisposition, error)
}

var _ RegistryCommands = (*PlanRegistry)(nil)
