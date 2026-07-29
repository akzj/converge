package model

import "testing"

func TestOperationFingerprintDeterministic(t *testing.T) {
	base := Operation{
		ID:          "transient-a",
		Key:         "stable",
		ConfigID:    "config-a",
		Provider:    "test",
		Action:      "apply",
		Input:       []byte(`{"b":2,"a":1}`),
		Phase:       PhaseCommit,
		Destructive: true,
		DependsOn:   []string{"z", "a"},
		Conditions: []Condition{
			{Name: "second", Input: []byte(`{"y":2,"x":1}`)},
			{Name: "first", Input: []byte(`true`)},
		},
		Timeout:     Duration(5),
		CancelMode:  CancelModeAsync,
		HandlerRef:  "handler",
		ConflictKey: "resource/x",
	}
	equivalent := base
	equivalent.ID = "transient-b"
	equivalent.Key = "another-runtime-key"
	equivalent.ConfigID = "config-b"
	equivalent.Input = []byte(`{ "a": 1, "b": 2 }`)
	equivalent.DependsOn = []string{"a", "z"}
	equivalent.Conditions = []Condition{base.Conditions[1], {
		Name: "second", Input: []byte(`{"x":1,"y":2}`),
	}}

	gotA, err := OperationFingerprint(base, "provider-digest")
	if err != nil {
		t.Fatal(err)
	}
	gotB, err := OperationFingerprint(equivalent, "provider-digest")
	if err != nil {
		t.Fatal(err)
	}
	if gotA != gotB {
		t.Fatalf("semantically equivalent operations differ: %q != %q", gotA, gotB)
	}
}

func TestOperationFingerprintSemanticChanges(t *testing.T) {
	base := Operation{Provider: "test", Action: "apply", Input: []byte(`{"a":1}`), Phase: PhaseCommit}
	baseFingerprint, err := OperationFingerprint(base, "digest-a")
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name           string
		mutate         func(*Operation)
		providerDigest string
	}{
		{name: "provider digest", providerDigest: "digest-b"},
		{name: "action", mutate: func(op *Operation) { op.Action = "remove" }},
		{name: "input", mutate: func(op *Operation) { op.Input = []byte(`{"a":2}`) }},
		{name: "dependency", mutate: func(op *Operation) { op.DependsOn = []string{"parent"} }},
		{name: "cancel mode", mutate: func(op *Operation) { op.CancelMode = CancelModeNone }},
		{name: "conflict key", mutate: func(op *Operation) { op.ConflictKey = "other" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changed := base
			digest := test.providerDigest
			if digest == "" {
				digest = "digest-a"
			}
			if test.mutate != nil {
				test.mutate(&changed)
			}
			got, err := OperationFingerprint(changed, digest)
			if err != nil {
				t.Fatal(err)
			}
			if got == baseFingerprint {
				t.Fatalf("semantic change did not change fingerprint: %q", got)
			}
		})
	}
}

func TestOperationFingerprintRejectsInvalidJSON(t *testing.T) {
	_, err := OperationFingerprint(Operation{Input: []byte(`{"broken"`)}, "digest")
	if err == nil {
		t.Fatal("expected invalid JSON error")
	}
}

func TestOperationFingerprintIncludesEffectRouting(t *testing.T) {
	base := Operation{Provider: "test", Key: "apply", ExecutionKind: ExecutionDirect, Action: "same"}
	direct, err := OperationFingerprint(base, "digest")
	if err != nil {
		t.Fatal(err)
	}
	effect := base
	effect.ExecutionKind = ExecutionEffectEnsure
	effect.EffectKey = "artifact"
	got, err := OperationFingerprint(effect, "digest")
	if err != nil {
		t.Fatal(err)
	}
	if got == direct {
		t.Fatal("effect routing did not change fingerprint")
	}
	effect.EffectKey = "another"
	changed, err := OperationFingerprint(effect, "digest")
	if err != nil {
		t.Fatal(err)
	}
	if changed == got {
		t.Fatal("effect key did not change fingerprint")
	}
}
