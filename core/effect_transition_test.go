package core

import (
	"testing"
	"time"
)

func TestValidateEffectTransitionMatrix(t *testing.T) {
	base := validEffect()
	tests := []struct {
		name     string
		from, to ExternalEffectState
		revision uint64
		valid    bool
	}{
		{"active completed", ExternalEffectActive, ExternalEffectCompleted, 2, true},
		{"active unknown", ExternalEffectActive, ExternalEffectUnknown, 2, true},
		{"cancel complete race", ExternalEffectCancelling, ExternalEffectCompleted, 2, true},
		{"unknown active", ExternalEffectUnknown, ExternalEffectActive, 2, true},
		{"completed regression", ExternalEffectCompleted, ExternalEffectActive, 2, false},
		{"cancelled ensuring", ExternalEffectCancelled, ExternalEffectEnsuring, 2, false},
		{"equal revision state change", ExternalEffectActive, ExternalEffectCompleted, 1, false},
		{"revision regression", ExternalEffectActive, ExternalEffectActive, 0, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			oldEffect, next := base, base
			oldEffect.State = test.from
			next.State, next.ExternalRevision = test.to, test.revision
			next.ResolutionRequired = effectStateRequiresResolution(test.to)
			if (ValidateEffectTransition(oldEffect, next, EffectTransitionExternalObservation) == nil) != test.valid {
				t.Fatalf("valid=%v", test.valid)
			}
		})
	}
	mutated := base
	mutated.ExternalJobID = "another"
	mutated.ExternalRevision++
	if err := ValidateEffectTransition(base, mutated, EffectTransitionExternalObservation); err == nil {
		t.Fatal("bound job replacement accepted")
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

func TestValidateEffectCoreIntentKeepsExternalRevision(t *testing.T) {
	for _, binding := range []EffectBindingState{EffectBindingUnbound, EffectBindingBound} {
		oldEffect := validEffect()
		oldEffect.Binding = binding
		if binding == EffectBindingUnbound {
			oldEffect.State, oldEffect.ExternalJobID, oldEffect.ExternalRevision = ExternalEffectEnsuring, "", 0
		}
		next := oldEffect
		next.State = ExternalEffectCancelRequested
		if err := ValidateEffectTransition(oldEffect, next, EffectTransitionCoreIntent); err != nil {
			t.Fatalf("binding=%s: %v", binding, err)
		}
		next.ExternalRevision++
		if err := ValidateEffectTransition(oldEffect, next, EffectTransitionCoreIntent); err == nil {
			t.Fatalf("binding=%s allowed revision mutation", binding)
		}
	}
}

func TestValidateReferenceTransitionMatrix(t *testing.T) {
	base := EffectReference{ID: "ref", EffectID: "effect", EffectKey: "key"}
	tests := []struct {
		from, to EffectReferenceState
		valid    bool
	}{
		{EffectReferenceEnsuring, EffectReferenceActive, true},
		{EffectReferenceEnsuring, EffectReferenceReleaseRequested, true},
		{EffectReferenceActive, EffectReferenceReleaseRequested, true},
		{EffectReferenceReleaseRequested, EffectReferenceReleased, true},
		{EffectReferenceReleased, EffectReferenceActive, false},
		{EffectReferenceActive, EffectReferenceEnsuring, false},
	}
	for _, test := range tests {
		oldRef, next := base, base
		oldRef.State, next.State = test.from, test.to
		if (ValidateReferenceTransition(oldRef, next) == nil) != test.valid {
			t.Fatalf("%s -> %s valid=%v", test.from, test.to, test.valid)
		}
	}
}

func TestValidateControlTransitionMatrix(t *testing.T) {
	base := EffectControl{ID: "control", EffectID: "effect", ReferenceID: "ref", Kind: EffectControlObserve}
	tests := []struct {
		from, to EffectControlState
		valid    bool
	}{
		{EffectControlPending, EffectControlInFlight, true},
		{EffectControlYielded, EffectControlInFlight, true},
		{EffectControlInFlight, EffectControlYielded, true},
		{EffectControlInFlight, EffectControlCompleted, true},
		{EffectControlInFlight, EffectControlPending, true},
		{EffectControlCompleted, EffectControlInFlight, false},
		{EffectControlPending, EffectControlCompleted, false},
	}
	for _, test := range tests {
		oldControl, next := base, base
		oldControl.State, next.State = test.from, test.to
		if test.from == EffectControlInFlight {
			oldControl.InFlightAttemptID, oldControl.PollRequestID, oldControl.LeaseExpiresAt = "old-attempt", "old-poll", time.Now().Add(time.Minute)
		}
		if test.to == EffectControlInFlight {
			next.InFlightAttemptID, next.PollRequestID, next.LeaseExpiresAt = "new-attempt", "new-poll", time.Now().Add(time.Minute)
		} else {
			next.InFlightAttemptID, next.PollRequestID, next.LeaseExpiresAt = "", "", time.Time{}
		}
		if (ValidateControlTransition(oldControl, next) == nil) != test.valid {
			t.Fatalf("%s -> %s valid=%v", test.from, test.to, test.valid)
		}
	}
}
