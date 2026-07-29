package core

import (
	"strconv"

	"github.com/cockroachdb/errors"

	"github.com/akzj/converge/pkg/model"
)

func ValidateEffectSnapshot(snapshot ExecutionSnapshot) error {
	effects := make(map[EffectID]ActiveEffect, len(snapshot.Effects))
	for _, effect := range snapshot.Effects {
		if effect.ID == "" {
			return errors.New("effect ID is empty")
		}
		if _, exists := effects[effect.ID]; exists {
			return errors.Errorf("duplicate effect ID %q", effect.ID)
		}
		if err := ValidateActiveEffect(effect); err != nil {
			return errors.Wrapf(err, "effect %q", effect.ID)
		}
		effects[effect.ID] = effect
	}
	references := make(map[ReferenceID]EffectReference, len(snapshot.EffectReferences))
	logicalSlots := make(map[string]ReferenceID, len(snapshot.EffectReferences))
	for _, reference := range snapshot.EffectReferences {
		if reference.ID == "" {
			return errors.New("reference ID is empty")
		}
		if _, exists := references[reference.ID]; exists {
			return errors.Errorf("duplicate reference ID %q", reference.ID)
		}
		if _, exists := effects[reference.EffectID]; !exists {
			return errors.Errorf("reference %q has missing effect %q", reference.ID, reference.EffectID)
		}
		if reference.EffectKey == "" {
			return errors.Errorf("reference %q has empty effect key", reference.ID)
		}
		if snapshot.Plan != nil && reference.ConfigID != snapshot.Plan.ConfigID {
			return errors.Errorf("reference %q does not belong to execution config", reference.ID)
		}
		if !validReferenceState(reference.State) {
			return errors.Errorf("reference %q has unknown state %q", reference.ID, reference.State)
		}
		slot := reference.ConfigID.Name + "\x00" + string(reference.PlanID) + "\x00" + strconv.FormatUint(uint64(reference.Generation), 10) + "\x00" + reference.EffectKey
		if prior, exists := logicalSlots[slot]; exists && prior != reference.ID {
			return errors.Errorf("logical effect slot has references %q and %q", prior, reference.ID)
		}
		logicalSlots[slot] = reference.ID
		references[reference.ID] = reference
	}
	controls := make(map[ControlRequestID]struct{}, len(snapshot.EffectControls))
	attempts, polls := make(map[model.AttemptID]struct{}), make(map[PollRequestID]struct{})
	for _, control := range snapshot.EffectControls {
		if control.ID == "" {
			return errors.New("control ID is empty")
		}
		if _, exists := controls[control.ID]; exists {
			return errors.Errorf("duplicate control ID %q", control.ID)
		}
		controls[control.ID] = struct{}{}
		if _, exists := effects[control.EffectID]; !exists {
			return errors.Errorf("control %q has missing effect %q", control.ID, control.EffectID)
		}
		reference, exists := references[control.ReferenceID]
		if !exists {
			return errors.Errorf("control %q has missing reference %q", control.ID, control.ReferenceID)
		}
		effect := effects[control.EffectID]
		if reference.EffectID != control.EffectID || control.ConfigID != reference.ConfigID || control.ProviderType != effect.ProviderType || control.ProviderDigest != effect.ProviderDigest {
			return errors.Errorf("control %q identity does not match effect/reference", control.ID)
		}
		if !validControlKind(control.Kind) || !validControlState(control.State) {
			return errors.Errorf("control %q has unknown kind/state", control.ID)
		}
		if err := validateEffectEntityCompatibility(effect, reference, control); err != nil {
			return errors.Wrapf(err, "control %q", control.ID)
		}
		if control.State == EffectControlInFlight {
			if control.InFlightAttemptID == "" || control.PollRequestID == "" || control.LeaseExpiresAt.IsZero() {
				return errors.Errorf("in-flight control %q has incomplete claim identity", control.ID)
			}
			if _, exists := attempts[control.InFlightAttemptID]; exists {
				return errors.Errorf("duplicate control attempt ID %q", control.InFlightAttemptID)
			}
			if _, exists := polls[control.PollRequestID]; exists {
				return errors.Errorf("duplicate poll request ID %q", control.PollRequestID)
			}
			attempts[control.InFlightAttemptID], polls[control.PollRequestID] = struct{}{}, struct{}{}
		} else if control.InFlightAttemptID != "" || control.PollRequestID != "" || !control.LeaseExpiresAt.IsZero() {
			return errors.Errorf("non-in-flight control %q retains claim identity", control.ID)
		}
	}
	return nil
}

func validReferenceState(state EffectReferenceState) bool {
	switch state {
	case EffectReferenceEnsuring, EffectReferenceActive, EffectReferenceReleaseRequested, EffectReferenceReleased:
		return true
	default:
		return false
	}
}

func validateEffectEntityCompatibility(effect ActiveEffect, reference EffectReference, control EffectControl) error {
	if reference.State == EffectReferenceActive && effect.Binding != EffectBindingBound {
		return errors.New("active reference requires bound effect")
	}
	switch control.Kind {
	case EffectControlEnsureRetry:
		if effect.Binding != EffectBindingUnbound || (effect.State != ExternalEffectEnsuring && effect.State != ExternalEffectUnknown && effect.State != ExternalEffectCancelRequested) {
			return errors.New("ensure retry requires unbound ensuring/unknown/cancel-requested effect")
		}
	case EffectControlEnsureReference:
		if effect.Binding != EffectBindingBound || reference.State != EffectReferenceEnsuring {
			return errors.New("ensure-reference requires bound effect and ensuring reference")
		}
	case EffectControlObserve:
		if effect.Binding != EffectBindingBound || reference.State != EffectReferenceActive ||
			(effect.State != ExternalEffectActive && effect.State != ExternalEffectUnknown) {
			return errors.New("observe requires active reference and bound active/unknown effect")
		}
	case EffectControlRelease:
		if effect.Binding != EffectBindingBound || reference.State != EffectReferenceReleaseRequested {
			return errors.New("release requires bound effect and release-requested reference")
		}
	case EffectControlObserveCancellation:
		if effect.Binding != EffectBindingBound ||
			(effect.State != ExternalEffectCancelRequested && effect.State != ExternalEffectCancelling && effect.State != ExternalEffectUnknown) {
			return errors.New("cancellation observation requires bound cancelling/unknown effect")
		}
	default:
		return errors.Errorf("unknown control kind %q", control.Kind)
	}
	return nil
}

func validControlKind(kind EffectControlKind) bool {
	switch kind {
	case EffectControlEnsureRetry, EffectControlEnsureReference, EffectControlObserve, EffectControlRelease, EffectControlObserveCancellation:
		return true
	default:
		return false
	}
}

func validControlState(state EffectControlState) bool {
	switch state {
	case EffectControlPending, EffectControlInFlight, EffectControlYielded, EffectControlCompleted:
		return true
	default:
		return false
	}
}

func ValidateActiveEffect(effect ActiveEffect) error {
	if effect.ProviderType == "" || effect.ProviderDigest == "" || effect.ConflictKey == "" || effect.ArtifactID == "" || effect.IdempotencyKey == "" || effect.SemanticFingerprint == "" {
		return errors.New("required semantic identity field is empty")
	}
	switch effect.Binding {
	case EffectBindingUnbound:
		if effect.ExternalJobID != "" || effect.ExternalRevision != 0 {
			return errors.New("unbound effect has external binding")
		}
		switch effect.State {
		case ExternalEffectEnsuring, ExternalEffectUnknown, ExternalEffectCancelRequested:
		case ExternalEffectFailed:
			if effect.ResolutionRequired {
				return errors.New("unbound failed effect cannot require resolution")
			}
		default:
			return errors.Errorf("state %q cannot be unbound", effect.State)
		}
	case EffectBindingBound:
		if effect.ExternalJobID == "" || effect.ExternalRevision == 0 {
			return errors.New("bound effect lacks external binding")
		}
		if effect.State == ExternalEffectEnsuring {
			return errors.New("bound effect cannot be ensuring")
		}
	default:
		return errors.Errorf("unknown binding state %q", effect.Binding)
	}
	if (effect.State == ExternalEffectCompleted || effect.State == ExternalEffectCancelled) && effect.ResolutionRequired {
		return errors.New("provider-confirmed terminal effect cannot require resolution")
	}
	if effectBlocksConflict(effect) != effect.ResolutionRequired {
		return errors.New("resolution-required flag does not match effect state")
	}
	return nil
}

func effectBlocksConflict(effect ActiveEffect) bool {
	switch effect.State {
	case ExternalEffectEnsuring, ExternalEffectActive, ExternalEffectCancelRequested, ExternalEffectCancelling, ExternalEffectUnknown:
		return true
	default:
		return effect.ResolutionRequired
	}
}

func OperationBlockedByEffect(operation model.Operation, plan *model.Plan, effect ActiveEffect, reference EffectReference) bool {
	if !effectBlocksConflict(effect) || operation.ConflictKey != effect.ConflictKey {
		return false
	}
	exactCurrentReference := plan != nil && reference.State == EffectReferenceActive &&
		reference.ConfigID == plan.ConfigID && reference.PlanID == plan.ID && reference.Generation == plan.Generation
	exactRetiredRelease := plan != nil && operation.ExecutionKind == model.ExecutionEffectRelease &&
		reference.State == EffectReferenceReleaseRequested && reference.ConfigID == plan.ConfigID &&
		operation.TargetReference == string(reference.ID)
	isControl := (exactCurrentReference || exactRetiredRelease) && operation.EffectKey == reference.EffectKey && reference.EffectID == effect.ID &&
		(operation.ExecutionKind == model.ExecutionEffectEnsure || operation.ExecutionKind == model.ExecutionEffectObserve || operation.ExecutionKind == model.ExecutionEffectRelease)
	return !isControl
}
