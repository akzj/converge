package core

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/akzj/converge/pkg/model"
)

// TestClassifyPlanChangeRandomized checks partition invariants over deterministic
// pseudo-random DAG mutations rather than only curated examples.
func TestClassifyPlanChangeRandomized(t *testing.T) {
	rng := rand.New(rand.NewPCG(0xC0FFEE, 0xBADC0DE))
	for iteration := 0; iteration < 500; iteration++ {
		oldOps := randomDAG(rng, 1+rng.IntN(12), "old")
		newOps := mutateDAG(rng, oldOps)
		oldPlan := testPlan(t, "digest", oldOps...)
		candidate := testPlan(t, "digest", newOps...)
		for _, node := range oldPlan.Nodes {
			statuses := []model.NodeStatus{model.NodePending, model.NodeReady, model.NodeCompleted, model.NodeFailed, model.NodeCancelled}
			node.Status = statuses[rng.IntN(len(statuses))]
		}
		change, err := ClassifyPlanChange(oldPlan, candidate)
		if err != nil {
			t.Fatalf("iteration %d: %v", iteration, err)
		}
		assertExactPartition(t, oldPlan, candidate, change)
	}
}

func randomDAG(rng *rand.Rand, size int, action string) []model.Operation {
	ops := make([]model.Operation, size)
	for i := range ops {
		key := model.OperationKey(fmt.Sprintf("op-%02d", i))
		ops[i] = model.Operation{Key: key, ExecutionKind: model.ExecutionDirect, Action: action, Input: []byte(fmt.Sprintf(`{"value":%d}`, i))}
		if i > 0 && rng.IntN(2) == 1 {
			ops[i].DependsOn = []string{string(ops[rng.IntN(i)].Key)}
		}
	}
	return ops
}

func mutateDAG(rng *rand.Rand, old []model.Operation) []model.Operation {
	var result []model.Operation
	for _, original := range old {
		if rng.IntN(5) == 0 {
			continue
		}
		op := original
		if rng.IntN(4) == 0 {
			op.Action = "changed"
		}
		result = append(result, op)
	}
	for i := 0; i < rng.IntN(4); i++ {
		result = append(result, model.Operation{Key: model.OperationKey(fmt.Sprintf("new-%02d", i)), ExecutionKind: model.ExecutionDirect, Action: "new"})
	}
	present := make(map[string]bool, len(result))
	for _, op := range result {
		present[string(op.Key)] = true
	}
	for i := range result {
		filtered := result[i].DependsOn[:0]
		for _, dependency := range result[i].DependsOn {
			if present[dependency] {
				filtered = append(filtered, dependency)
			}
		}
		result[i].DependsOn = filtered
	}
	return result
}

func assertExactPartition(t *testing.T, oldPlan, candidate *model.Plan, change PlanChange) {
	t.Helper()
	oldSeen := make(map[model.OperationKey]int)
	for _, keys := range [][]model.OperationKey{change.Carry, change.Drop, change.Cancel, change.Drain} {
		for _, key := range keys {
			oldSeen[key]++
		}
	}
	for key := range oldPlan.Nodes {
		if oldSeen[key] != 1 {
			t.Fatalf("old key %q partition count=%d change=%#v", key, oldSeen[key], change)
		}
	}
	newSeen := make(map[model.OperationKey]int)
	for _, key := range change.Carry {
		newSeen[key]++
	}
	for _, key := range change.Add {
		newSeen[key]++
	}
	for key := range candidate.Nodes {
		if newSeen[key] != 1 {
			t.Fatalf("new key %q partition count=%d change=%#v", key, newSeen[key], change)
		}
	}
}
