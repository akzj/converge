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
	base := EffectControl{ID: "control", EffectID: "effect", ReferenceID: "ref", Kind: EffectControlObserve, TargetKind: EffectTargetMaintenance}
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

func TestValidateControlShapeRequiresExplicitCompleteTarget(t *testing.T) {
	base := EffectControl{ID: "control", EffectID: "effect", ReferenceID: "ref", Kind: EffectControlObserve, State: EffectControlPending}
	for _, control := range []EffectControl{
		base,
		func() EffectControl { c := base; c.TargetKind = EffectTargetPlanNode; c.PlanID = "plan"; return c }(),
		func() EffectControl {
			c := base
			c.TargetKind = EffectTargetPlanNode
			c.PlanID = "plan"
			c.Generation = 1
			return c
		}(),
		func() EffectControl { c := base; c.TargetKind = EffectTargetMaintenance; c.PlanID = "plan"; return c }(),
	} {
		if err := validateControlShape(control); err == nil {
			t.Fatalf("accepted incomplete control: %#v", control)
		}
	}
	planBound := base
	planBound.TargetKind, planBound.PlanID, planBound.Generation, planBound.OperationKey = EffectTargetPlanNode, "plan", 1, "observe"
	if err := validateControlShape(planBound); err != nil {
		t.Fatalf("complete plan-bound control rejected: %v", err)
	}
	maintenance := base
	maintenance.TargetKind = EffectTargetMaintenance
	if err := validateControlShape(maintenance); err != nil {
		t.Fatalf("maintenance control rejected: %v", err)
	}
}

func TestValidateEffectEnsureResultBindsAndMatchesIdentity(t *testing.T) {
	oldEffect := validEffect()
	oldEffect.Binding, oldEffect.State, oldEffect.ExternalJobID, oldEffect.ExternalRevision = EffectBindingUnbound, ExternalEffectEnsuring, "", 0
	bound := oldEffect
	bound.Binding, bound.State, bound.ExternalJobID, bound.ExternalRevision = EffectBindingBound, ExternalEffectActive, "job", 1
	if err := ValidateEffectTransition(oldEffect, bound, EffectTransitionEnsureResult); err != nil {
		t.Fatalf("valid ensure result rejected: %v", err)
	}
	mismatched := bound
	mismatched.EnsureSpec = []byte(`{"different":true}`)
	if err := ValidateEffectTransition(oldEffect, mismatched, EffectTransitionEnsureResult); err == nil {
		t.Fatal("mismatched ensure spec accepted")
	}
	boundFromActive := bound
	boundFromActive.Binding, boundFromActive.State, boundFromActive.ExternalRevision = EffectBindingBound, ExternalEffectActive, 2
	if err := ValidateEffectTransition(bound, boundFromActive, EffectTransitionEnsureResult); err == nil {
		t.Fatal("ensure result allowed on already-bound effect")
	}
}

func TestObservationsCannotBind(t *testing.T) {
	oldEffect := validEffect()
	oldEffect.Binding, oldEffect.State, oldEffect.ExternalJobID, oldEffect.ExternalRevision = EffectBindingUnbound, ExternalEffectEnsuring, "", 0
	bound := oldEffect
	bound.Binding, bound.State, bound.ExternalJobID, bound.ExternalRevision = EffectBindingBound, ExternalEffectActive, "job", 1
	if err := ValidateEffectTransition(oldEffect, bound, EffectTransitionExternalObservation); err == nil {
		t.Fatal("external observation should not be able to bind effect")
	}
}

func TestExternalObservationCannotBeIntentTransition(t *testing.T) {
	oldEffect := validEffect()
	next := oldEffect
	next.State = ExternalEffectCancelRequested
	next.ExternalRevision = oldEffect.ExternalRevision
	if err := ValidateEffectTransition(oldEffect, next, EffectTransitionExternalObservation); err == nil {
		t.Fatal("external observation should not accept same-revision state change")
	}
	next.ExternalRevision++
	if err := ValidateEffectTransition(oldEffect, next, EffectTransitionExternalObservation); err != nil {
		t.Fatalf("observation with higher revision should be valid: %v", err)
	}
}
