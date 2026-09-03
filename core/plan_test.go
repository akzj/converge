package core

import (
	"reflect"
	"testing"

	"github.com/akzj/converge/pkg/model"
)

func testPlan(t *testing.T, digest string, operations ...model.Operation) *model.Plan {
	t.Helper()
	for i := range operations {
		if operations[i].ExecutionKind == "" {
			operations[i].ExecutionKind = model.ExecutionDirect
		}
	}
	spec := []byte(`{"desired":true}`)
	plan, err := BuildCandidate(model.ConfigID{Name: "config"}, model.DesiredState{
		ConfigID: model.ConfigID{Name: "config"}, ProviderType: "test", Version: 1,
		Spec: spec, Digest: model.DesiredSpecDigest(spec),
	}, "test", digest, operations)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func TestBuildCandidateValidatesIdentityAndGraph(t *testing.T) {
	tests := []struct {
		name string
		ops  []model.Operation
	}{
		{name: "missing key", ops: []model.Operation{{ID: "legacy"}}},
		{name: "duplicate key", ops: []model.Operation{{Key: "same"}, {Key: "same"}}},
		{name: "missing dependency", ops: []model.Operation{{Key: "child", DependsOn: []string{"missing"}}}},
		{name: "cycle", ops: []model.Operation{{Key: "a", DependsOn: []string{"b"}}, {Key: "b", DependsOn: []string{"a"}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := BuildCandidate(model.ConfigID{Name: "config"}, model.DesiredState{}, "test", "digest", test.ops); err == nil {
				t.Fatal("expected candidate validation error")
			}
		})
	}
}

func TestClassifyPlanChangeIsExhaustive(t *testing.T) {
	oldPlan := testPlan(t, "digest",
		model.Operation{Key: "carry", Action: "same"},
		model.Operation{Key: "drop", Action: "old"},
		model.Operation{Key: "cancel", Action: "old", CancelMode: model.CancelModeSafe},
		model.Operation{Key: "drain", Action: "old", CancelMode: model.CancelModeNone},
		model.Operation{Key: "changed", Action: "old"},
	)
	oldPlan.Nodes["carry"].Status = model.NodeRunning
	oldPlan.Nodes["drop"].Status = model.NodePending
	oldPlan.Nodes["cancel"].Status = model.NodeRunning
	oldPlan.Nodes["drain"].Status = model.NodeRunning
	oldPlan.Nodes["changed"].Status = model.NodeCompleted
	oldPlan.Nodes["carry"].AttemptID = "attempt-carry"

	candidate := testPlan(t, "digest",
		model.Operation{Key: "carry", Action: "same"},
		model.Operation{Key: "changed", Action: "new"},
		model.Operation{Key: "added", Action: "new"},
	)
	change, err := ClassifyPlanChange(oldPlan, candidate)
	if err != nil {
		t.Fatal(err)
	}
	want := PlanChange{
		Carry:  []model.OperationKey{"carry"},
		Drop:   []model.OperationKey{"changed", "drop"},
		Cancel: []model.OperationKey{"cancel"},
		Drain:  []model.OperationKey{"drain"},
		Add:    []model.OperationKey{"added", "changed"},
	}
	if !reflect.DeepEqual(change, want) {
		t.Fatalf("classification mismatch:\nwant: %#v\n got: %#v", want, change)
	}
}

func TestClassifyPlanChangeProviderUpgradeDisablesCarry(t *testing.T) {
	op := model.Operation{Key: "apply", Action: "same"}
	oldPlan := testPlan(t, "digest-a", op)
	oldPlan.Nodes["apply"].Status = model.NodeRunning
	candidate := testPlan(t, "digest-b", op)

	change, err := ClassifyPlanChange(oldPlan, candidate)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(change.Cancel, []model.OperationKey{"apply"}) || !reflect.DeepEqual(change.Add, []model.OperationKey{"apply"}) {
		t.Fatalf("provider upgrade classification mismatch: %#v", change)
	}
}

func TestClassifyPlanChangeCarriesCompletedEquivalentNode(t *testing.T) {
	op := model.Operation{Key: "apply", Action: "same"}
	oldPlan := testPlan(t, "digest", op)
	oldPlan.Nodes["apply"].Status = model.NodeCompleted
	candidate := testPlan(t, "digest", op)

	change, err := ClassifyPlanChange(oldPlan, candidate)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(change.Carry, []model.OperationKey{"apply"}) || len(change.Add) != 0 {
		t.Fatalf("equivalent completed node was not carried: %#v", change)
	}
}

func TestClassifyPlanChangeRejectsCarryWhenAncestorChanges(t *testing.T) {
	oldPlan := testPlan(t, "digest", model.Operation{Key: "parent", Action: "v1"}, model.Operation{Key: "child", Action: "same", DependsOn: []string{"parent"}})
	oldPlan.Nodes["parent"].Status = model.NodeCompleted
	oldPlan.Nodes["child"].Status = model.NodeCompleted
	candidate := testPlan(t, "digest", model.Operation{Key: "parent", Action: "v2"}, model.Operation{Key: "child", Action: "same", DependsOn: []string{"parent"}})
	change, err := ClassifyPlanChange(oldPlan, candidate)
	if err != nil {
		t.Fatal(err)
	}
	if len(change.Carry) != 0 {
		t.Fatalf("child with changed ancestor was carried: %#v", change)
	}
}

func TestClassifyPlanChangeCarryStateEligibility(t *testing.T) {
	tests := []struct {
		status  model.NodeStatus
		attempt model.AttemptID
		want    bool
	}{
		{model.NodePending, "", true}, {model.NodeReady, "", true},
		{model.NodeRunning, "attempt", true}, {model.NodeRunning, "", false},
		{model.NodeCompleted, "", true}, {model.NodeFailed, "", false},
		{model.NodeCancelled, "", false}, {model.NodeCancelling, "attempt", false},
		{model.NodeDraining, "attempt", false},
	}
	for _, test := range tests {
		t.Run(string(test.status)+"/"+string(test.attempt), func(t *testing.T) {
			op := model.Operation{Key: "apply", Action: "same"}
			oldPlan := testPlan(t, "digest", op)
			oldPlan.Nodes["apply"].Status, oldPlan.Nodes["apply"].AttemptID = test.status, test.attempt
			change, err := ClassifyPlanChange(oldPlan, testPlan(t, "digest", op))
			if err != nil {
				t.Fatal(err)
			}
			if got := len(change.Carry) == 1; got != test.want {
				t.Fatalf("carry=%v, want %v: %#v", got, test.want, change)
			}
		})
	}
}

func TestBuildCandidateNormalizesProviderAndConflictKey(t *testing.T) {
	plan, err := BuildCandidate(model.ConfigID{Name: "example"}, model.DesiredState{}, "provider-a", "digest", []model.Operation{{Key: "apply", ExecutionKind: model.ExecutionDirect}})
	if err != nil {
		t.Fatal(err)
	}
	op := plan.Nodes["apply"].Operation
	if op.Provider != "provider-a" || op.ConflictKey != "config/example" {
		t.Fatalf("candidate was not normalized: %#v", op)
	}
	if _, err := BuildCandidate(model.ConfigID{Name: "example"}, model.DesiredState{}, "provider-a", "digest", []model.Operation{{Key: "apply", Provider: "provider-b"}}); err == nil {
		t.Fatal("expected mismatched provider error")
	}
}

func TestClassifyWaitingNodeOnSupersession(t *testing.T) {
	oldPlan := testPlan(t, "digest", model.Operation{Key: "wait", Action: "old"})
	oldPlan.Nodes["wait"].Status = model.NodeWaiting
	oldPlan.Nodes["wait"].AttemptID = "waiting-attempt"
	candidate := testPlan(t, "digest", model.Operation{Key: "wait", Action: "new"})
	change, err := ClassifyPlanChange(oldPlan, candidate)
	if err != nil {
		t.Fatal(err)
	}
	if len(change.Drop) != 1 || change.Drop[0] != "wait" || len(change.Add) != 1 || change.Add[0] != "wait" {
		t.Fatalf("waiting supersession classification=%#v", change)
	}
}

func TestClassifyWaitingOnControlNodeOnSupersession(t *testing.T) {
	oldPlan := testPlan(t, "digest", model.Operation{Key: "ensure", ExecutionKind: model.ExecutionEffectEnsure, EffectKey: "download"})
	oldPlan.Nodes["ensure"].Status = model.NodeWaitingOnControl
	candidate := testPlan(t, "digest", model.Operation{Key: "ensure", ExecutionKind: model.ExecutionEffectEnsure, EffectKey: "download", Input: []byte(`{"version":2}`)})
	change, err := ClassifyPlanChange(oldPlan, candidate)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(change.Drop, []model.OperationKey{"ensure"}) || !reflect.DeepEqual(change.Add, []model.OperationKey{"ensure"}) {
		t.Fatalf("waiting-on-control supersession classification=%#v", change)
	}
}

func TestBuildCandidateValidatesEffectOperationTopology(t *testing.T) {
	tests := []struct {
		name string
		ops  []model.Operation
	}{
		{name: "direct effect key", ops: []model.Operation{{Key: "direct", ExecutionKind: model.ExecutionDirect, EffectKey: "bad"}}},
		{name: "ensure missing key", ops: []model.Operation{{Key: "ensure", ExecutionKind: model.ExecutionEffectEnsure}}},
		{name: "observe without ensure", ops: []model.Operation{{Key: "observe", ExecutionKind: model.ExecutionEffectObserve, EffectKey: "download"}}},
		{name: "observe lacks dependency", ops: []model.Operation{{Key: "ensure", ExecutionKind: model.ExecutionEffectEnsure, EffectKey: "download"}, {Key: "observe", ExecutionKind: model.ExecutionEffectObserve, EffectKey: "download"}}},
		{name: "duplicate ensure", ops: []model.Operation{{Key: "a", ExecutionKind: model.ExecutionEffectEnsure, EffectKey: "download"}, {Key: "b", ExecutionKind: model.ExecutionEffectEnsure, EffectKey: "download"}}},
		{name: "unknown kind", ops: []model.Operation{{Key: "bad", ExecutionKind: "invalid"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := BuildCandidate(model.ConfigID{Name: "config"}, model.DesiredState{}, "test", "digest", test.ops); err == nil {
				t.Fatal("expected topology validation error")
			}
		})
	}

	valid := []model.Operation{
		{Key: "ensure", ExecutionKind: model.ExecutionEffectEnsure, EffectKey: "download"},
		{Key: "observe", ExecutionKind: model.ExecutionEffectObserve, EffectKey: "download", DependsOn: []string{"ensure"}},
		{Key: "release", ExecutionKind: model.ExecutionEffectRelease, EffectKey: "download", ReleaseTarget: model.ReleaseCurrentPlan, TargetReference: "ref", DependsOn: []string{"observe"}},
	}
	if _, err := BuildCandidate(model.ConfigID{Name: "config"}, model.DesiredState{}, "test", "digest", valid); err != nil {
		t.Fatalf("valid effect topology rejected: %v", err)
	}
}

func TestBuildCandidateRejectsInvalidEffectReleaseTopology(t *testing.T) {
	tests := []struct {
		name string
		ops  []model.Operation
	}{
		{name: "current plan release missing target", ops: []model.Operation{{Key: "release", ExecutionKind: model.ExecutionEffectRelease, EffectKey: "download"}}},
		{name: "retired reference missing target", ops: []model.Operation{{Key: "release", ExecutionKind: model.ExecutionEffectRelease, ReleaseTarget: model.ReleaseRetiredReference, EffectKey: "download"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := BuildCandidate(model.ConfigID{Name: "config"}, model.DesiredState{}, "test", "digest", test.ops); err == nil {
				t.Fatal("expected release topology error")
			}
		})
	}
}

func TestBuildCandidateAllowsRetiredReferenceReleasePlan(t *testing.T) {
	ops := []model.Operation{{Key: "release-old", ExecutionKind: model.ExecutionEffectRelease, ReleaseTarget: model.ReleaseRetiredReference, EffectKey: "download", TargetReference: "old-reference"}}
	if _, err := BuildCandidate(model.ConfigID{Name: "config"}, model.DesiredState{}, "test", "digest", ops); err != nil {
		t.Fatalf("retired reference release rejected: %v", err)
	}
}
