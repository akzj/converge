package core

import "github.com/cockroachdb/errors"

type EffectTransitionOrigin string

const (
	EffectTransitionCoreIntent          EffectTransitionOrigin = "core_intent"
	EffectTransitionEnsureResult        EffectTransitionOrigin = "ensure_result"
	EffectTransitionExternalObservation EffectTransitionOrigin = "external_observation"
	// EffectTransitionCoreResolution marks a Core-local resolution state change
	// (e.g. Active -> Unknown after a transport error). It must not advance the
	// external revision, which only the external service may do.
	EffectTransitionCoreResolution EffectTransitionOrigin = "core_resolution"
)

// ValidateEffectTransition rejects state regression, identity mutation, binding
// replacement, and stale/contradictory external observations.
func ValidateEffectTransition(oldEffect, next ActiveEffect, origin EffectTransitionOrigin) error {
	if oldEffect.ID != next.ID || oldEffect.ArtifactID != next.ArtifactID ||
		oldEffect.IdempotencyKey != next.IdempotencyKey || oldEffect.SemanticFingerprint != next.SemanticFingerprint ||
		oldEffect.ProviderType != next.ProviderType || oldEffect.ProviderDigest != next.ProviderDigest || oldEffect.ConflictKey != next.ConflictKey {
		return errors.New("immutable effect identity changed")
	}
	if oldEffect.Binding == EffectBindingBound {
		if next.Binding != EffectBindingBound || oldEffect.ExternalJobID != next.ExternalJobID {
			return errors.New("bound external job identity changed")
		}
	}
	if next.ExternalRevision < oldEffect.ExternalRevision {
		return errors.New("external revision regressed")
	}
	if oldEffect.Binding != next.Binding {
		if origin != EffectTransitionEnsureResult || oldEffect.Binding != EffectBindingUnbound || next.Binding != EffectBindingBound {
			return errors.New("only ensure result may bind an unbound effect")
		}
	}
	switch origin {
	case EffectTransitionEnsureResult:
		if oldEffect.Binding != EffectBindingUnbound {
			return errors.New("ensure result requires a previously unbound effect")
		}
		if oldEffect.State != ExternalEffectEnsuring && oldEffect.State != ExternalEffectUnknown && oldEffect.State != ExternalEffectCancelRequested {
			return errors.Errorf("ensure result from illegal state %q", oldEffect.State)
		}
		if oldEffect.IdempotencyKey != next.IdempotencyKey || oldEffect.ArtifactID != next.ArtifactID || string(oldEffect.EnsureSpec) != string(next.EnsureSpec) {
			return errors.New("ensure result must match original immutable ensure request")
		}
		if next.Binding == EffectBindingBound {
			if next.ExternalJobID == "" || next.ExternalRevision == 0 {
				return errors.New("bound ensure result lacks external binding")
			}
		} else if next.State != ExternalEffectUnknown && next.State != ExternalEffectFailed && next.State != ExternalEffectEnsuring && next.State != ExternalEffectCancelRequested {
			return errors.Errorf("unbound ensure result has illegal state %q", next.State)
		}
	case EffectTransitionExternalObservation:
		if next.ExternalRevision == oldEffect.ExternalRevision && oldEffect.State != next.State {
			return errors.New("effect state changed at equal external revision")
		}
	case EffectTransitionCoreIntent:
		if next.ExternalRevision != oldEffect.ExternalRevision {
			return errors.New("core intent cannot change external revision")
		}
		if next.State != ExternalEffectCancelRequested {
			return errors.New("unsupported core intent transition")
		}
	case EffectTransitionCoreResolution:
		if next.ExternalRevision != oldEffect.ExternalRevision {
			return errors.New("core resolution cannot change external revision")
		}
		if next.State != ExternalEffectUnknown && oldEffect.State != ExternalEffectUnknown {
			return errors.New("core resolution must enter or leave Unknown")
		}
	default:
		return errors.Errorf("unknown transition origin %q", origin)
	}
	if oldEffect.State == next.State {
		return ValidateActiveEffect(next)
	}
	if !legalEffectStateTransition(oldEffect, next) {
		return errors.Errorf("illegal effect transition %q -> %q", oldEffect.State, next.State)
	}
	return ValidateActiveEffect(next)
}

func legalEffectStateTransition(oldEffect, next ActiveEffect) bool {
	switch oldEffect.State {
	case ExternalEffectEnsuring:
		return next.State == ExternalEffectActive || next.State == ExternalEffectUnknown ||
			next.State == ExternalEffectCancelRequested || next.State == ExternalEffectFailed
	case ExternalEffectActive:
		return next.State == ExternalEffectCompleted || next.State == ExternalEffectCancelRequested ||
			next.State == ExternalEffectUnknown || next.State == ExternalEffectFailed
	case ExternalEffectCancelRequested:
		return next.State == ExternalEffectCancelling || next.State == ExternalEffectCompleted ||
			next.State == ExternalEffectCancelled || next.State == ExternalEffectUnknown || next.State == ExternalEffectFailed
	case ExternalEffectCancelling:
		return next.State == ExternalEffectCompleted || next.State == ExternalEffectCancelled ||
			next.State == ExternalEffectUnknown || next.State == ExternalEffectFailed
	case ExternalEffectUnknown:
		return next.State == ExternalEffectActive || next.State == ExternalEffectCompleted ||
			next.State == ExternalEffectCancelled || next.State == ExternalEffectFailed || next.State == ExternalEffectCancelRequested
	case ExternalEffectCompleted, ExternalEffectCancelled, ExternalEffectFailed:
		return false
	default:
		return false
	}
}

func ValidateReferenceTransition(oldReference, next EffectReference) error {
	if oldReference.ID != next.ID || oldReference.EffectID != next.EffectID ||
		oldReference.ConfigID != next.ConfigID || oldReference.PlanID != next.PlanID ||
		oldReference.Generation != next.Generation || oldReference.EffectKey != next.EffectKey {
		return errors.New("immutable reference identity changed")
	}
	if oldReference.State == next.State {
		return nil
	}
	switch oldReference.State {
	case EffectReferenceEnsuring:
		if next.State != EffectReferenceActive && next.State != EffectReferenceReleaseRequested {
			return errors.Errorf("illegal reference transition %q -> %q", oldReference.State, next.State)
		}
	case EffectReferenceActive:
		if next.State != EffectReferenceReleaseRequested {
			return errors.Errorf("illegal reference transition %q -> %q", oldReference.State, next.State)
		}
	case EffectReferenceReleaseRequested:
		if next.State != EffectReferenceReleased {
			return errors.Errorf("illegal reference transition %q -> %q", oldReference.State, next.State)
		}
	case EffectReferenceReleased:
		return errors.New("released reference is terminal")
	default:
		return errors.Errorf("unknown reference state %q", oldReference.State)
	}
	return nil
}

func validateControlShape(control EffectControl) error {
	// Plan-bound controls advance a DAG node and therefore require the complete
	// immutable NodeIdentity. Maintenance controls never advance a DAG node.
	switch control.TargetKind {
	case EffectTargetMaintenance:
		if control.PlanID != "" || control.Generation != 0 || control.OperationKey != "" {
			return errors.New("maintenance control carries plan identity")
		}
	case EffectTargetPlanNode:
		if control.PlanID == "" || control.Generation == 0 || control.OperationKey == "" {
			return errors.New("plan-node control lacks complete node identity")
		}
	default:
		return errors.Errorf("control has unknown target kind %q", control.TargetKind)
	}
	if control.State == EffectControlInFlight {
		if control.InFlightAttemptID == "" || control.PollRequestID == "" || control.LeaseExpiresAt.IsZero() {
			return errors.New("in-flight control lacks claim identity")
		}
		return nil
	}
	if control.InFlightAttemptID != "" || control.PollRequestID != "" || !control.LeaseExpiresAt.IsZero() {
		return errors.New("non-in-flight control retains claim identity")
	}
	return nil
}

func ValidateControlTransition(oldControl, next EffectControl) error {
	if oldControl.ID != next.ID || oldControl.ConfigID != next.ConfigID ||
		oldControl.ProviderType != next.ProviderType || oldControl.ProviderDigest != next.ProviderDigest ||
		oldControl.Kind != next.Kind || oldControl.EffectID != next.EffectID || oldControl.ReferenceID != next.ReferenceID ||
		oldControl.PlanID != next.PlanID || oldControl.Generation != next.Generation ||
		oldControl.OperationKey != next.OperationKey {
		return errors.New("immutable control identity changed")
	}
	if oldControl.State == next.State {
		return validateControlShape(next)
	}
	switch oldControl.State {
	case EffectControlPending, EffectControlYielded:
		if next.State != EffectControlInFlight {
			return errors.Errorf("illegal control transition %q -> %q", oldControl.State, next.State)
		}
	case EffectControlInFlight:
		if next.State != EffectControlYielded && next.State != EffectControlCompleted && next.State != EffectControlPending {
			return errors.Errorf("illegal control transition %q -> %q", oldControl.State, next.State)
		}
	case EffectControlCompleted:
		return errors.New("completed control is terminal")
	default:
		return errors.Errorf("unknown control state %q", oldControl.State)
	}
	return validateControlShape(next)
}
