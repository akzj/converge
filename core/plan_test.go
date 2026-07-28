package core

import (
	"reflect"
	"testing"

	"github.com/akzj/converge/pkg/model"
)

func testPlan(t *testing.T, digest string, operations ...model.Operation) *model.Plan {
	t.Helper()
	plan, err := BuildCandidate(model.ConfigID{Name: "config"}, model.DesiredState{Version: 1, Digest: "desired"}, digest, operations)
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
			if _, err := BuildCandidate(model.ConfigID{Name: "config"}, model.DesiredState{}, "digest", test.ops); err == nil {
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
