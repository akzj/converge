package model

import (
	"encoding/json"
	"testing"
	"time"
)

// --- Duration.UnmarshalJSON ---

func TestDurationMarshalJSONRoundtrip(t *testing.T) {
	tests := []struct {
		name     string
		input    string // JSON string like `"5s"`
		expected time.Duration
	}{
		{name: "seconds", input: `"5s"`, expected: 5 * time.Second},
		{name: "milliseconds", input: `"100ms"`, expected: 100 * time.Millisecond},
		{name: "minutes", input: `"1m0s"`, expected: time.Minute},
		{name: "hours", input: `"2h0m0s"`, expected: 2 * time.Hour},
		{name: "combined", input: `"1h30m0s"`, expected: 90 * time.Minute},
		{name: "zero", input: `"0s"`, expected: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var d Duration
			if err := json.Unmarshal([]byte(test.input), &d); err != nil {
				t.Fatalf("unmarshal %s: %v", test.input, err)
			}
			if time.Duration(d) != test.expected {
				t.Fatalf("got %v, want %v", time.Duration(d), test.expected)
			}
			// Marshal back — Go's time.Duration.String() always produces the
			// canonical form with seconds (e.g. "1m0s", not "1m").
			got, err := json.Marshal(d)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if string(got) != test.input {
				t.Fatalf("marshal got %s, want %s", string(got), test.input)
			}
		})
	}
}

func TestDurationUnmarshalJSONRejectsInvalid(t *testing.T) {
	invalid := []string{
		`"abc"`,
		`""`,
		`123`,       // number, not string
		`"5"`,       // no unit
		`null`,
	}
	for _, input := range invalid {
		t.Run(input, func(t *testing.T) {
			var d Duration
			if err := json.Unmarshal([]byte(input), &d); err == nil {
				t.Fatalf("expected error for %s, got %v", input, time.Duration(d))
			}
		})
	}
}

// --- Graph.ReadyNodes ---

func TestGraphReadyNodesEmpty(t *testing.T) {
	g := &Graph{}
	ready := g.ReadyNodes()
	if len(ready) != 0 {
		t.Fatalf("expected 0 ready nodes, got %d", len(ready))
	}
}

func TestGraphReadyNodesAllPendingNoDeps(t *testing.T) {
	g := &Graph{
		Nodes: map[string]*Node{
			"a": {Operation: Operation{Key: "a"}, Status: NodePending},
			"b": {Operation: Operation{Key: "b"}, Status: NodePending},
		},
	}
	ready := g.ReadyNodes()
	if len(ready) != 2 {
		t.Fatalf("expected 2 ready nodes, got %d", len(ready))
	}
	for _, n := range ready {
		if n.Status != NodeReady {
			t.Fatalf("ready node %s status=%s, want ready", n.Operation.Key, n.Status)
		}
	}
}

func TestGraphReadyNodesDependsOnCompleted(t *testing.T) {
	g := &Graph{
		Nodes: map[string]*Node{
			"dep":  {Operation: Operation{Key: "dep", DependsOn: nil}, Status: NodeCompleted},
			"leaf": {Operation: Operation{Key: "leaf", DependsOn: []string{"dep"}}, Status: NodePending},
		},
	}
	ready := g.ReadyNodes()
	if len(ready) != 1 || ready[0].Operation.Key != "leaf" {
		t.Fatalf("expected 1 ready (leaf), got %#v", ready)
	}
}

func TestGraphReadyNodesBlockedByUnmetDependency(t *testing.T) {
	g := &Graph{
		Nodes: map[string]*Node{
			"dep":  {Operation: Operation{Key: "dep"}, Status: NodeRunning},
			"leaf": {Operation: Operation{Key: "leaf", DependsOn: []string{"dep"}}, Status: NodePending},
		},
	}
	ready := g.ReadyNodes()
	if len(ready) != 0 {
		t.Fatalf("expected 0 ready (dep not completed), got %d", len(ready))
	}
}

func TestGraphReadyNodesBlockedByMissingDependency(t *testing.T) {
	g := &Graph{
		Nodes: map[string]*Node{
			"leaf": {Operation: Operation{Key: "leaf", DependsOn: []string{"missing"}}, Status: NodePending},
		},
	}
	ready := g.ReadyNodes()
	if len(ready) != 0 {
		t.Fatalf("expected 0 ready (dep missing), got %d", len(ready))
	}
}

func TestGraphReadyNodesNonPendingSkipped(t *testing.T) {
	g := &Graph{
		Nodes: map[string]*Node{
			"running":   {Operation: Operation{Key: "running"}, Status: NodeRunning},
			"completed": {Operation: Operation{Key: "completed"}, Status: NodeCompleted},
			"pending":   {Operation: Operation{Key: "pending"}, Status: NodePending},
		},
	}
	ready := g.ReadyNodes()
	if len(ready) != 1 || ready[0].Operation.Key != "pending" {
		t.Fatalf("expected 1 ready (pending), got %#v", ready)
	}
}

func TestGraphReadyNodesTransitive(t *testing.T) {
	g := &Graph{
		Nodes: map[string]*Node{
			"a": {Operation: Operation{Key: "a"}, Status: NodeCompleted},
			"b": {Operation: Operation{Key: "b", DependsOn: []string{"a"}}, Status: NodePending},
			"c": {Operation: Operation{Key: "c", DependsOn: []string{"b"}}, Status: NodePending},
		},
	}
	ready := g.ReadyNodes()
	if len(ready) != 1 || ready[0].Operation.Key != "b" {
		t.Fatalf("expected 1 ready (b), got %#v", ready)
	}
}

// --- Graph.Validate ---

func TestGraphValidateNil(t *testing.T) {
	var g *Graph
	if err := g.Validate(); err != nil {
		t.Fatalf("nil graph: %v", err)
	}
}

func TestGraphValidateEmpty(t *testing.T) {
	g := &Graph{}
	if err := g.Validate(); err != nil {
		t.Fatalf("empty graph: %v", err)
	}
}

func TestGraphValidateNoIssues(t *testing.T) {
	g := &Graph{
		Nodes: map[string]*Node{
			"a": {Operation: Operation{Key: "a"}},
			"b": {Operation: Operation{Key: "b", DependsOn: []string{"a"}}},
		},
	}
	if err := g.Validate(); err != nil {
		t.Fatalf("valid graph: %v", err)
	}
}

func TestGraphValidateDanglingDependency(t *testing.T) {
	g := &Graph{
		Nodes: map[string]*Node{
			"a": {Operation: Operation{Key: "a", DependsOn: []string{"missing"}}},
		},
	}
	if err := g.Validate(); err == nil {
		t.Fatal("expected dangling dependency error")
	}
}

func TestGraphValidateCycle(t *testing.T) {
	g := &Graph{
		Nodes: map[string]*Node{
			"a": {Operation: Operation{Key: "a", DependsOn: []string{"b"}}},
			"b": {Operation: Operation{Key: "b", DependsOn: []string{"a"}}},
		},
	}
	if err := g.Validate(); err == nil {
		t.Fatal("expected cycle error")
	}
}

func TestGraphValidateSelfCycle(t *testing.T) {
	g := &Graph{
		Nodes: map[string]*Node{
			"a": {Operation: Operation{Key: "a", DependsOn: []string{"a"}}},
		},
	}
	if err := g.Validate(); err == nil {
		t.Fatal("expected self-cycle error")
	}
}

func TestGraphValidateDiamondNoCycle(t *testing.T) {
	g := &Graph{
		Nodes: map[string]*Node{
			"root": {Operation: Operation{Key: "root"}},
			"left": {Operation: Operation{Key: "left", DependsOn: []string{"root"}}},
			"right": {Operation: Operation{Key: "right", DependsOn: []string{"root"}}},
			"leaf":  {Operation: Operation{Key: "leaf", DependsOn: []string{"left", "right"}}},
		},
	}
	if err := g.Validate(); err != nil {
		t.Fatalf("valid diamond graph: %v", err)
	}
}

func TestGraphValidateCycleAndDangling(t *testing.T) {
	// Both errors should be reported (dangling checked first).
	g := &Graph{
		Nodes: map[string]*Node{
			"a": {Operation: Operation{Key: "a", DependsOn: []string{"missing"}}},
			"b": {Operation: Operation{Key: "b", DependsOn: []string{"a"}}},
			"c": {Operation: Operation{Key: "c", DependsOn: []string{"b"}}},
		},
	}
	if err := g.Validate(); err == nil {
		t.Fatal("expected error (dangling)")
	}
}
