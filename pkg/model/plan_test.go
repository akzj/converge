package model

import "testing"

func TestPlanCloneIsDeepCopy(t *testing.T) {
	original := &Plan{
		ID: "plan-1",
		Nodes: map[OperationKey]*Node{
			"apply": {
				Operation: Operation{
					Key:       "apply",
					Input:     []byte(`{"value":1}`),
					DependsOn: []string{"prepare"},
					Conditions: []Condition{{
						Name: "ready", Input: []byte(`{"check":true}`),
					}},
				},
				Status:    NodeRunning,
				AttemptID: "attempt-1",
			},
		},
	}

	clone := original.Clone()
	clone.Nodes["apply"].Status = NodeCompleted
	clone.Nodes["apply"].Operation.Input[0] = '['
	clone.Nodes["apply"].Operation.DependsOn[0] = "changed"
	clone.Nodes["apply"].Operation.Conditions[0].Input[0] = '['
	delete(clone.Nodes, "apply")

	node := original.Nodes["apply"]
	if node.Status != NodeRunning {
		t.Fatalf("clone status mutation affected original: %s", node.Status)
	}
	if string(node.Operation.Input) != `{"value":1}` {
		t.Fatalf("clone input mutation affected original: %s", node.Operation.Input)
	}
	if node.Operation.DependsOn[0] != "prepare" {
		t.Fatalf("clone dependency mutation affected original: %s", node.Operation.DependsOn[0])
	}
	if string(node.Operation.Conditions[0].Input) != `{"check":true}` {
		t.Fatalf("clone condition mutation affected original: %s", node.Operation.Conditions[0].Input)
	}
	if original.Nodes["apply"] == nil {
		t.Fatal("clone map mutation affected original")
	}
}
