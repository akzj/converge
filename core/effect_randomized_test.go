package core

import (
	"math/rand/v2"
	"testing"
)

// TestEffectTransitionRounds is a property test that picks random old and new
// states and verifies the transition validator never contradicts its own
// legalStateTransition definition.
func TestEffectTransitionRounds(t *testing.T) {
	states := []ExternalEffectState{
		ExternalEffectEnsuring, ExternalEffectActive,
		ExternalEffectCancelRequested, ExternalEffectCancelling,
		ExternalEffectCompleted, ExternalEffectCancelled,
		ExternalEffectFailed, ExternalEffectUnknown,
	}
	origins := []EffectTransitionOrigin{
		EffectTransitionCoreIntent,
		EffectTransitionEnsureResult,
		EffectTransitionExternalObservation,
	}
	rng := rand.New(rand.NewPCG(42, 0xDEAD))
	for iteration := 0; iteration < 200; iteration++ {
		oldState := states[rng.IntN(len(states))]
		newState := states[rng.IntN(len(states))]
		origin := origins[rng.IntN(len(origins))]

		oldEffect := validEffect()
		oldEffect.State = oldState
		oldEffect.ResolutionRequired = effectStateRequiresResolution(oldState)
		if oldState == ExternalEffectEnsuring || oldState == ExternalEffectUnknown || oldState == ExternalEffectCancelRequested {
			oldEffect.Binding, oldEffect.ExternalJobID, oldEffect.ExternalRevision = EffectBindingUnbound, "", 0
		}

		next := oldEffect
		next.State = newState
		next.ResolutionRequired = effectStateRequiresResolution(newState)
		if newState == ExternalEffectActive || newState == ExternalEffectCompleted || newState == ExternalEffectCancelled || newState == ExternalEffectFailed || newState == ExternalEffectCancelling {
			next.Binding, next.ExternalJobID, next.ExternalRevision = EffectBindingBound, "job", 2
		}

		err := ValidateEffectTransition(oldEffect, next, origin)
		if oldState == newState {
			if origin == EffectTransitionExternalObservation && err != nil {
				t.Errorf("iteration %d: same-state %s via observation should be a no-op but got %v", iteration, oldState, err)
			}
		} else if !legalEffectStateTransition(oldEffect, next) {
			if err == nil {
				t.Errorf("iteration %d: illegal transition %s -> %s via %s should have been rejected but was accepted",
					iteration, oldState, newState, origin)
			}
		}
	}
}

func TestReferenceStateTransitionRounds(t *testing.T) {
	states := []EffectReferenceState{
		EffectReferenceEnsuring, EffectReferenceActive,
		EffectReferenceReleaseRequested, EffectReferenceReleased,
	}
	rng := rand.New(rand.NewPCG(0xCAFE, 0xBABE))
	for iteration := 0; iteration < 50; iteration++ {
		oldS := states[rng.IntN(len(states))]
		newS := states[rng.IntN(len(states))]
		oldRef := EffectReference{ID: "ref", EffectID: "effect", EffectKey: "key", State: oldS}
		newRef := EffectReference{ID: "ref", EffectID: "effect", EffectKey: "key", State: newS}
		err := ValidateReferenceTransition(oldRef, newRef)
		if oldS == newS {
			if err != nil {
				t.Errorf("iteration %d: same-state %s rejected", iteration, oldS)
			}
		} else if oldS == EffectReferenceReleased {
			if err == nil {
				t.Errorf("iteration %d: terminal %s -> %s accepted", iteration, oldS, newS)
			}
		} else if oldS == EffectReferenceEnsuring && (newS != EffectReferenceActive && newS != EffectReferenceReleaseRequested) {
			if err == nil {
				t.Errorf("iteration %d: illegal ensuring -> %s accepted", iteration, newS)
			}
		}
	}
}

func effectStateRequiresResolution(state ExternalEffectState) bool {
	switch state {
	case ExternalEffectEnsuring, ExternalEffectActive, ExternalEffectCancelRequested, ExternalEffectCancelling, ExternalEffectUnknown:
		return true
	default:
		return false
	}
}
