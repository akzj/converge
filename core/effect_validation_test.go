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
			e.Binding = EffectBindingUnbound
			e.ExternalJobID = ""
			e.ExternalRevision = 0
			e.State = ExternalEffectEnsuring
			return e
		}(), valid: true},
		{name: "unbound unknown", effect: func() ActiveEffect {
			e := validEffect()
			e.Binding = EffectBindingUnbound
			e.ExternalJobID = ""
			e.ExternalRevision = 0
			e.State = ExternalEffectUnknown
			return e
		}(), valid: true},
		{name: "unbound authoritative rejection", effect: func() ActiveEffect {
			e := validEffect()
			e.Binding = EffectBindingUnbound
			e.ExternalJobID = ""
			e.ExternalRevision = 0
			e.State = ExternalEffectFailed
			e.ResolutionRequired = false
			return e
		}(), valid: true},
		{name: "unbound unresolved failure", effect: func() ActiveEffect {
			e := validEffect()
			e.Binding = EffectBindingUnbound
			e.ExternalJobID = ""
			e.ExternalRevision = 0
			e.State = ExternalEffectFailed
			return e
		}()},
		{name: "terminal requires resolution mismatch", effect: func() ActiveEffect { e := validEffect(); e.State = ExternalEffectCompleted; return e }()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateActiveEffect(test.effect)
			if (err == nil) != test.valid {
				t.Fatalf("valid=%v error=%v", test.valid, err)
			}
		})
	}
}

func TestValidateEffectSnapshotIdentityAndReferences(t *testing.T) {
	effect := validEffect()
	reference := EffectReference{ID: "ref", EffectID: effect.ID, ConfigID: model.ConfigID{Name: "config"}, PlanID: "plan", Generation: 1, EffectKey: "artifact", State: EffectReferenceActive}
	control := EffectControl{ID: "control", ConfigID: reference.ConfigID, ProviderType: effect.ProviderType, ProviderDigest: effect.ProviderDigest, Kind: EffectControlObserve, State: EffectControlInFlight, EffectID: effect.ID, ReferenceID: reference.ID, InFlightAttemptID: "attempt", PollRequestID: "poll", LeaseExpiresAt: time.Now().Add(time.Minute)}
	valid := ExecutionSnapshot{Effects: []ActiveEffect{effect}, EffectReferences: []EffectReference{reference}, EffectControls: []EffectControl{control}}
	if err := ValidateEffectSnapshot(valid); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		mutate func(*ExecutionSnapshot)
	}{
		{name: "duplicate effect", mutate: func(s *ExecutionSnapshot) { s.Effects = append(s.Effects, effect) }},
		{name: "missing effect reference", mutate: func(s *ExecutionSnapshot) { s.EffectReferences[0].EffectID = "missing" }},
		{name: "missing control reference", mutate: func(s *ExecutionSnapshot) { s.EffectControls[0].ReferenceID = "missing" }},
		{name: "incomplete claim", mutate: func(s *ExecutionSnapshot) { s.EffectControls[0].PollRequestID = "" }},
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

func TestEffectBarrierAllowsOnlyExactControl(t *testing.T) {
	effect := validEffect()
	reference := EffectReference{ID: "ref", EffectID: effect.ID, EffectKey: "download"}
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
			if got := OperationBlockedByEffect(test.operation, effect, reference); got != test.blocked {
				t.Fatalf("blocked=%v, want %v", got, test.blocked)
			}
		})
	}
}
