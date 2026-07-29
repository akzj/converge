package core

import "github.com/cockroachdb/errors"

// ValidateEffectTransition rejects state regression, identity mutation, binding
// replacement, and stale/contradictory external revisions.
func ValidateEffectTransition(oldEffect, next ActiveEffect) error {
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
	if next.ExternalRevision == oldEffect.ExternalRevision && oldEffect.State != next.State {
		return errors.New("effect state changed at equal external revision")
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

func ValidateControlTransition(oldControl, next EffectControl) error {
	if oldControl.ID != next.ID || oldControl.ConfigID != next.ConfigID ||
		oldControl.ProviderType != next.ProviderType || oldControl.ProviderDigest != next.ProviderDigest ||
		oldControl.Kind != next.Kind || oldControl.EffectID != next.EffectID || oldControl.ReferenceID != next.ReferenceID {
		return errors.New("immutable control identity changed")
	}
	if oldControl.State == next.State {
		return nil
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
	return nil
}
