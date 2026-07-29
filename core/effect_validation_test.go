package core

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/akzj/converge/pkg/model"
)

func validEffect() ActiveEffect {
	return ActiveEffect{ID: "effect", Binding: EffectBindingBound, ExternalJobID: "job", ArtifactID: "sha256:x", IdempotencyKey: "idem", SemanticFingerprint: "fp", EnsureSpec: json.RawMessage(`{"url":"x"}`), ProviderType: "download", ProviderDigest: "v1", ConflictKey: "artifact/x", State: ExternalEffectActive, ResolutionRequired: true, ExternalRevision: 1}
}

func TestValidateActiveEffectBindingMatrix(t *testing.T) {
	tests := []struct {
		name   string
		effect ActiveEffect
		valid  bool
	}{
		{name: "bound active", effect: validEffect(), valid: true},
		{name: "bound missing job", effect: func() ActiveEffect { e := validEffect(); e.ExternalJobID = ""; return e }()},
		{name: "unbound ensuring", effect: func() ActiveEffect {
			e := validEffect()
			e.Binding, e.State, e.ExternalJobID, e.ExternalRevision = EffectBindingUnbound, ExternalEffectEnsuring, "", 0
			return e
		}(), valid: true},
		{name: "unbound unknown", effect: func() ActiveEffect {
			e := validEffect()
			e.Binding, e.State, e.ExternalJobID, e.ExternalRevision = EffectBindingUnbound, ExternalEffectUnknown, "", 0
			return e
		}(), valid: true},
		{name: "unbound authoritative rejection", effect: func() ActiveEffect {
			e := validEffect()
			e.Binding, e.State, e.ExternalJobID, e.ExternalRevision, e.ResolutionRequired = EffectBindingUnbound, ExternalEffectFailed, "", 0, false
			return e
		}(), valid: true},
		{name: "unbound unresolved failure", effect: func() ActiveEffect {
			e := validEffect()
			e.Binding, e.State, e.ExternalJobID, e.ExternalRevision = EffectBindingUnbound, ExternalEffectFailed, "", 0
			return e
		}()},
		{name: "terminal requires resolution", effect: func() ActiveEffect { e := validEffect(); e.State = ExternalEffectCompleted; return e }()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if (ValidateActiveEffect(test.effect) == nil) != test.valid {
				t.Fatalf("valid=%v", test.valid)
			}
		})
	}
}

func validEffectSnapshot() ExecutionSnapshot {
	effect := validEffect()
	plan := &model.Plan{ID: "plan", ConfigID: model.ConfigID{Name: "config"}, Generation: 2}
	reference := EffectReference{ID: "ref", EffectID: effect.ID, ConfigID: plan.ConfigID, PlanID: plan.ID, Generation: plan.Generation, EffectKey: "artifact", State: EffectReferenceActive}
	control := EffectControl{ID: "control", ConfigID: reference.ConfigID, ProviderType: effect.ProviderType, ProviderDigest: effect.ProviderDigest, Kind: EffectControlObserve, State: EffectControlInFlight, EffectID: effect.ID, ReferenceID: reference.ID, InFlightAttemptID: "attempt", PollRequestID: "poll", LeaseExpiresAt: time.Now().Add(time.Minute)}
	return ExecutionSnapshot{Plan: plan, Effects: []ActiveEffect{effect}, EffectReferences: []EffectReference{reference}, EffectControls: []EffectControl{control}}
}

func TestValidateEffectSnapshotIdentityAndReferences(t *testing.T) {
	valid := validEffectSnapshot()
	if err := ValidateEffectSnapshot(valid); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*ExecutionSnapshot)
	}{
		{name: "duplicate effect", mutate: func(s *ExecutionSnapshot) { s.Effects = append(s.Effects, s.Effects[0]) }},
		{name: "missing effect reference", mutate: func(s *ExecutionSnapshot) { s.EffectReferences[0].EffectID = "missing" }},
		{name: "reference wrong config", mutate: func(s *ExecutionSnapshot) { s.EffectReferences[0].ConfigID.Name = "wrong" }},
		{name: "reference unknown state", mutate: func(s *ExecutionSnapshot) { s.EffectReferences[0].State = "invalid" }},
		{name: "duplicate logical slot", mutate: func(s *ExecutionSnapshot) {
			duplicate := s.EffectReferences[0]
			duplicate.ID = "other"
			s.EffectReferences = append(s.EffectReferences, duplicate)
		}},
		{name: "missing control reference", mutate: func(s *ExecutionSnapshot) { s.EffectControls[0].ReferenceID = "missing" }},
		{name: "control wrong provider", mutate: func(s *ExecutionSnapshot) { s.EffectControls[0].ProviderDigest = "wrong" }},
		{name: "control effect mismatch", mutate: func(s *ExecutionSnapshot) {
			other := s.Effects[0]
			other.ID = "other"
			s.Effects = append(s.Effects, other)
			s.EffectControls[0].EffectID = other.ID
		}},
		{name: "control unknown kind", mutate: func(s *ExecutionSnapshot) { s.EffectControls[0].Kind = "invalid" }},
		{name: "control unknown state", mutate: func(s *ExecutionSnapshot) { s.EffectControls[0].State = "invalid" }},
		{name: "incomplete claim", mutate: func(s *ExecutionSnapshot) { s.EffectControls[0].PollRequestID = "" }},
		{name: "pending retains claim", mutate: func(s *ExecutionSnapshot) { s.EffectControls[0].State = EffectControlPending }},
		{name: "duplicate poll", mutate: func(s *ExecutionSnapshot) {
			duplicate := s.EffectControls[0]
			duplicate.ID = "second"
			s.EffectControls = append(s.EffectControls, duplicate)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := cloneExecutionSnapshot(valid)
			test.mutate(&snapshot)
			if err := ValidateEffectSnapshot(snapshot); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestEffectEntityCompatibilityMatrix(t *testing.T) {
	baseEffect := validEffect()
	baseReference := EffectReference{ID: "ref", EffectID: baseEffect.ID, ConfigID: model.ConfigID{Name: "config"}, EffectKey: "download", State: EffectReferenceActive}
	tests := []struct {
		name           string
		kind           EffectControlKind
		binding        EffectBindingState
		effectState    ExternalEffectState
		referenceState EffectReferenceState
		valid          bool
	}{
		{"observe active", EffectControlObserve, EffectBindingBound, ExternalEffectActive, EffectReferenceActive, true},
		{"observe ensuring ref", EffectControlObserve, EffectBindingBound, ExternalEffectActive, EffectReferenceEnsuring, false},
		{"observe unbound", EffectControlObserve, EffectBindingUnbound, ExternalEffectUnknown, EffectReferenceActive, false},
		{"ensure retry unbound", EffectControlEnsureRetry, EffectBindingUnbound, ExternalEffectUnknown, EffectReferenceEnsuring, true},
		{"ensure retry bound", EffectControlEnsureRetry, EffectBindingBound, ExternalEffectActive, EffectReferenceActive, false},
		{"ensure reference", EffectControlEnsureReference, EffectBindingBound, ExternalEffectActive, EffectReferenceEnsuring, true},
		{"ensure reference active", EffectControlEnsureReference, EffectBindingBound, ExternalEffectActive, EffectReferenceActive, false},
		{"release requested", EffectControlRelease, EffectBindingBound, ExternalEffectActive, EffectReferenceReleaseRequested, true},
		{"release active", EffectControlRelease, EffectBindingBound, ExternalEffectActive, EffectReferenceActive, false},
		{"observe cancelling", EffectControlObserveCancellation, EffectBindingBound, ExternalEffectCancelling, EffectReferenceReleaseRequested, true},
		{"observe cancellation active", EffectControlObserveCancellation, EffectBindingBound, ExternalEffectActive, EffectReferenceReleaseRequested, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			effect := baseEffect
			effect.Binding, effect.State = test.binding, test.effectState
			if test.binding == EffectBindingUnbound {
				effect.ExternalJobID, effect.ExternalRevision = "", 0
			}
			reference := baseReference
			reference.State = test.referenceState
			if (validateEffectEntityCompatibility(effect, reference, EffectControl{Kind: test.kind}) == nil) != test.valid {
				t.Fatalf("valid=%v", test.valid)
			}
		})
	}
}

func TestEffectBarrierAllowsOnlyExactControl(t *testing.T) {
	effect := validEffect()
	plan := &model.Plan{ID: "plan", ConfigID: model.ConfigID{Name: "config"}, Generation: 1}
	reference := EffectReference{ID: "ref", EffectID: effect.ID, EffectKey: "download", ConfigID: plan.ConfigID, PlanID: plan.ID, Generation: plan.Generation, State: EffectReferenceActive}
	tests := []struct {
		name      string
		operation model.Operation
		blocked   bool
	}{
		{name: "direct blocked", operation: model.Operation{ExecutionKind: model.ExecutionDirect, ConflictKey: effect.ConflictKey}, blocked: true},
		{name: "new effect blocked", operation: model.Operation{ExecutionKind: model.ExecutionEffectEnsure, EffectKey: "other", ConflictKey: effect.ConflictKey}, blocked: true},
		{name: "exact observe allowed", operation: model.Operation{ExecutionKind: model.ExecutionEffectObserve, EffectKey: "download", ConflictKey: effect.ConflictKey}},
		{name: "unrelated conflict allowed", operation: model.Operation{ExecutionKind: model.ExecutionDirect, ConflictKey: "other"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := OperationBlockedByEffect(test.operation, plan, effect, reference); got != test.blocked {
				t.Fatalf("blocked=%v, want %v", got, test.blocked)
			}
		})
	}
}

func TestEffectBarrierRejectsNonExactReferences(t *testing.T) {
	effect := validEffect()
	plan := &model.Plan{ID: "plan", ConfigID: model.ConfigID{Name: "config"}, Generation: 2}
	operation := model.Operation{ExecutionKind: model.ExecutionEffectObserve, EffectKey: "download", ConflictKey: effect.ConflictKey}
	base := EffectReference{ID: "ref", EffectID: effect.ID, EffectKey: "download", ConfigID: plan.ConfigID, PlanID: plan.ID, Generation: plan.Generation, State: EffectReferenceActive}
	tests := []struct {
		name   string
		mutate func(*EffectReference)
	}{
		{name: "ensuring", mutate: func(r *EffectReference) { r.State = EffectReferenceEnsuring }},
		{name: "old generation", mutate: func(r *EffectReference) { r.Generation-- }},
		{name: "wrong plan", mutate: func(r *EffectReference) { r.PlanID = "old" }},
		{name: "wrong config", mutate: func(r *EffectReference) { r.ConfigID.Name = "other" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reference := base
			test.mutate(&reference)
			if !OperationBlockedByEffect(operation, plan, effect, reference) {
				t.Fatal("non-exact reference crossed barrier")
			}
		})
	}
}
