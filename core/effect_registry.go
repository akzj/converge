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
	effect.Binding = EffectBindingBound
	effect.ExternalJobID = result.ExternalJobID
	effect.ExternalRevision = result.ExternalRevision
	effect.State = ExternalEffectActive
	effect.ResolutionRequired = true
	state.effects[effect.ID] = effect

	reference := state.references[identity.EffectIdentity.ReferenceID]
	reference.State = EffectReferenceActive
	state.references[reference.ID] = reference

	observeControl := EffectControl{
		ID:       ControlRequestID("observe-" + string(identity.EffectIdentity.EffectID)),
		ConfigID: identity.EffectIdentity.ConfigID, ProviderType: effect.ProviderType,
		ProviderDigest: effect.ProviderDigest, Kind: EffectControlObserve,
		State: EffectControlPending, EffectID: effect.ID, ReferenceID: identity.EffectIdentity.ReferenceID,
		NextCheckAt: time.Now().Add(5 * time.Second),
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
	control.State = EffectControlInFlight
	control.InFlightAttemptID = attemptID
	control.PollRequestID = pollID
	control.LeaseExpiresAt = leaseUntil
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
	effect := state.effects[identity.EffectIdentity.EffectID]
	if effect.ID == "" {
		return TransitionStale, nil
	}
	reference := state.references[identity.EffectIdentity.ReferenceID]
	if reference.ID == "" {
		return TransitionStale, nil
	}
	switch observation.Disposition {
	case DispositionStillActive:
		control.State = EffectControlYielded
		control.NextCheckAt = observation.NextCheckAt
		control.InFlightAttemptID = ""
		control.PollRequestID = ""
		control.LeaseExpiresAt = time.Time{}
		effect.State = ExternalEffectActive
		effect.ExternalRevision = observation.ExternalRevision
		state.controls[control.ID] = control
		state.effects[effect.ID] = effect

	case DispositionCompleted:
		control.State = EffectControlCompleted
		control.InFlightAttemptID = ""
		control.PollRequestID = ""
		control.LeaseExpiresAt = time.Time{}
		effect.State = ExternalEffectCompleted
		effect.ResolutionRequired = false
		effect.ExternalRevision = observation.ExternalRevision
		// Observe completion does not request release; the plan's release
		// node (or supersession) owns ReferenceReleaseRequested.
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
