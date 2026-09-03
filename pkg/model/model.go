// Package model defines the fundamental, language-neutral data types for
// the Converge reconciliation engine.
package model

import (
	"encoding/json"
	"time"

	"github.com/cockroachdb/errors"
)

// ---------------------------------------------------------------------------
// Identity
// ---------------------------------------------------------------------------
// PlanID uniquely identifies one immutable execution plan.
type PlanID string

// Generation is a monotonically increasing plan generation within a config.
type Generation uint64

// OperationKey is a provider-defined stable logical operation identity.
type OperationKey string

// AttemptID uniquely identifies one execution attempt.
type AttemptID string

// ConfigID uniquely identifies a managed configuration within a Converge instance.
type ConfigID struct {
	Name string // globally unique within Converge
}

// ResourceID identifies a concrete resource managed by a Provider.
type ResourceID struct {
	Type, Namespace, Name, InstanceKey string
}

// ---------------------------------------------------------------------------
// Phase
// ---------------------------------------------------------------------------

// Phase represents the lifecycle stage of an Operation.
type Phase string

const (
	PhasePrepare Phase = "prepare" // side-effect-free preparation
	PhaseWait    Phase = "wait"    // waiting for a runtime condition
	PhaseCommit  Phase = "commit"  // destructive, potentially irreversible mutation
	PhaseVerify  Phase = "verify"  // read-back from real system
	PhaseCleanup Phase = "cleanup" // removal of artifacts owned by a superseded version
)

// ---------------------------------------------------------------------------
// StepResult
// ---------------------------------------------------------------------------

// StepState is the outcome of executing one Operation.
type StepState string

const (
	StepCompleted StepState = "completed"
	StepWaiting   StepState = "waiting"
	StepFailed    StepState = "failed"
	StepCancelled StepState = "cancelled"
)

// StepResult is returned by a Provider after executing an Operation.
type StepResult struct {
	State       StepState `json:"state"`
	Code        string    `json:"code,omitempty"`          // stable error code
	Reason      string    `json:"reason,omitempty"`        // human-readable explanation
	Retryable   bool      `json:"retryable"`               // true=may retry; false=wait for new desired
	NextCheckAt time.Time `json:"next_check_at,omitempty"` // when Waiting should be re-evaluated
}

// ---------------------------------------------------------------------------
// Condition
// ---------------------------------------------------------------------------

// Condition is a runtime guard evaluated before an Operation becomes ready.
type Condition struct {
	Name     string     `json:"name"` // e.g. "no_active_players"
	Resource ResourceID `json:"resource,omitempty"`
	Input    []byte     `json:"input,omitempty"` // condition-specific parameters
}

// OperationExecutionKind selects Core's generic execution path without
// interpreting provider-specific Action values.
type OperationExecutionKind string

const (
	ExecutionDirect        OperationExecutionKind = "direct"
	ExecutionEffectEnsure  OperationExecutionKind = "effect_ensure"
	ExecutionEffectObserve OperationExecutionKind = "effect_observe"
	ExecutionEffectRelease OperationExecutionKind = "effect_release"
)

type ReleaseTargetKind string

const (
	ReleaseCurrentPlan      ReleaseTargetKind = "current_plan"
	ReleaseRetiredReference ReleaseTargetKind = "retired_reference"
)

// ---------------------------------------------------------------------------
// CancelMode
// ---------------------------------------------------------------------------

// CancelMode expresses whether an in-flight Operation can be safely interrupted.
type CancelMode string

const (
	CancelModeSafe  CancelMode = "safe"  // cancellable at any point (download, inspect)
	CancelModeAsync CancelMode = "async" // cancellable between sub-steps
	CancelModeNone  CancelMode = "none"  // not cancellable once started
)

// ---------------------------------------------------------------------------
// Operation
// ---------------------------------------------------------------------------

// Operation is the smallest atomic execution unit in Converge.
type Operation struct {
	ID              string                 `json:"id"` // deprecated: use Key for stable logical identity
	Key             OperationKey           `json:"key,omitempty"`
	ExecutionKind   OperationExecutionKind `json:"execution_kind"`
	EffectKey       string                 `json:"effect_key,omitempty"`
	ReleaseTarget   ReleaseTargetKind      `json:"release_target,omitempty"`
	TargetReference string                 `json:"target_reference,omitempty"`
	Fingerprint     string                 `json:"fingerprint,omitempty"`
	ConflictKey     string                 `json:"conflict_key,omitempty"`
	ConfigID        string                 `json:"config_id"` // owning configuration name
	Provider        string                 `json:"provider"`  // target provider name
	Action          string                 `json:"action"`    // "provision"|"deprovision"|"reconcile"
	Input           []byte                 `json:"input,omitempty"`
	Phase           Phase                  `json:"phase"`
	Destructive     bool                   `json:"destructive"`
	DependsOn       []string               `json:"depends_on,omitempty"` // operation keys; string retained for source compatibility
	Conditions      []Condition            `json:"conditions,omitempty"`
	Timeout         Duration               `json:"timeout,omitempty"`
	CancelMode      CancelMode             `json:"cancel_mode"`
	HandlerRef      string                 `json:"handler_ref,omitempty"`
}

// Duration is a JSON-serializable time.Duration.
type Duration time.Duration

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	td, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(td)
	return nil
}

// ---------------------------------------------------------------------------
// Desired / Observed
// ---------------------------------------------------------------------------

// DesiredState is the full specification for one configuration.
type DesiredState struct {
	ConfigID     ConfigID `json:"config_id"`
	ProviderType string   `json:"provider_type"` // which provider handles this (e.g. "nginx.config")
	Version      uint64   `json:"version"`
	Spec         []byte   `json:"spec"`                 // provider-specific desired spec (JSON)
	Digest       string   `json:"digest"`               // sha256 of Spec
	DependsOn    []string `json:"depends_on,omitempty"` // config names that must converge first
}

// ObservedState is what a Provider reads from the real system.
type ObservedState struct {
	ConfigID   ConfigID `json:"config_id"`
	Version    string   `json:"version,omitempty"`
	Properties []byte   `json:"properties,omitempty"`
	Digest     string   `json:"digest,omitempty"` // sha256 of Properties
	Present    bool     `json:"present"`          // false = resource does not exist
}

// ---------------------------------------------------------------------------
// RecordedState (State Store)
// ---------------------------------------------------------------------------

// RecordedState is persisted after each successful convergence.
type RecordedState struct {
	ConfigID       ConfigID     `json:"config_id"`
	ProviderType   string       `json:"provider_type"` // which provider owns this config
	DesiredVersion uint64       `json:"desired_version"`
	DesiredDigest  string       `json:"desired_digest"`
	HandlerDigest  string       `json:"handler_digest"`
	HandlerRef     string       `json:"handler_ref"`
	Status         ConfigStatus `json:"status"`
	UpdatedAt      time.Time    `json:"updated_at"`
}

// ---------------------------------------------------------------------------
// DAG
// ---------------------------------------------------------------------------

// NodeStatus tracks execution progress of an Operation within the DAG.
type NodeStatus string

const (
	NodePending NodeStatus = "pending"
	NodeReady   NodeStatus = "ready"
	NodeRunning NodeStatus = "running"
	NodeWaiting NodeStatus = "waiting"
	// NodeWaitingOnControl marks an effect node that has activated its durable
	// EffectControl and is waiting for the EffectControl scheduler to drive it.
	// It consumes no execSem slot and creates no DAG provider Attempt.
	NodeWaitingOnControl NodeStatus = "waiting_on_control"
	NodeCancelling       NodeStatus = "cancelling"
	NodeDraining         NodeStatus = "draining"
	NodeCompleted        NodeStatus = "completed"
	NodeFailed           NodeStatus = "failed"
	NodeCancelled        NodeStatus = "cancelled"
)

// Node wraps an Operation with its runtime status.
type Node struct {
	Operation  Operation  `json:"operation"`
	Status     NodeStatus `json:"status"`
	AttemptID  AttemptID  `json:"attempt_id,omitempty"`
	RetryCount int        `json:"-"` // number of times this node has been retried
}

// Graph is a DAG of Operations ready for scheduling.
type Graph struct {
	Nodes map[string]*Node `json:"nodes"`
}

// ReadyNodes returns all nodes whose dependencies are satisfied.
func (g *Graph) ReadyNodes() []*Node {
	var ready []*Node
	for _, n := range g.Nodes {
		if n.Status != NodePending {
			continue
		}
		if allDepsCompleted(n.Operation.DependsOn, g.Nodes) {
			n.Status = NodeReady
			ready = append(ready, n)
		}
	}
	return ready
}

func allDepsCompleted(deps []string, nodes map[string]*Node) bool {
	for _, depID := range deps {
		if n, ok := nodes[depID]; !ok || n.Status != NodeCompleted {
			return false
		}
	}
	return true
}

// Validate checks the graph for structural errors:
//   - cycles (nodes that depend on each other transitively)
//   - dangling intra-graph dependencies (DependsOn references a node ID
//     that does not exist in this graph)
//
// Cross-config dependencies (references to node IDs outside this graph)
// are silently skipped — they are resolved by the global graph scheduler.
func (g *Graph) Validate() error {
	if g == nil || len(g.Nodes) == 0 {
		return nil
	}

	// --- Check 1: dangling intra-graph dependencies ---
	var dangles []string
	for id, n := range g.Nodes {
		for _, dep := range n.Operation.DependsOn {
			if _, ok := g.Nodes[dep]; !ok {
				dangles = append(dangles, id+" depends on missing node: "+dep)
			}
		}
	}
	if len(dangles) > 0 {
		return errors.Errorf("graph has %d dangling dependency(ies): %v", len(dangles), dangles)
	}

	// --- Check 2: cycle detection (DFS three-color) ---
	type color int
	const (
		white color = 0
		gray  color = 1
		black color = 2
	)
	colors := make(map[string]color, len(g.Nodes))
	for id := range g.Nodes {
		colors[id] = white
	}

	var cycles []string
	var dfs func(id string)
	dfs = func(id string) {
		colors[id] = gray
		for _, dep := range g.Nodes[id].Operation.DependsOn {
			// All DependsOn references are guaranteed to exist after check 1.
			switch colors[dep] {
			case gray:
				cycles = append(cycles, dep+"->"+id)
			case white:
				dfs(dep)
			}
		}
		colors[id] = black
	}

	for id := range g.Nodes {
		if colors[id] == white {
			dfs(id)
		}
	}

	if len(cycles) > 0 {
		return errors.Errorf("graph contains %d cycle(s): %v", len(cycles), cycles)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Event
// ---------------------------------------------------------------------------

// Event is sent from Executor to Orchestrator after an Operation completes.
type Event struct {
	EventID    string        `json:"event_id,omitempty"`
	Sequence   uint64        `json:"sequence,omitempty"`
	PlanID     PlanID        `json:"plan_id,omitempty"`
	Generation Generation    `json:"generation,omitempty"`
	NodeKey    OperationKey  `json:"node_key,omitempty"`
	AttemptID  AttemptID     `json:"attempt_id,omitempty"`
	NodeID     string        `json:"node_id,omitempty"` // deprecated
	ConfigID   string        `json:"config_id"`
	State      StepState     `json:"state"`
	Result     StepResult    `json:"result"`
	Observed   ObservedState `json:"observed,omitempty"`
}

// ---------------------------------------------------------------------------
// Configuration (managed unit)
// ---------------------------------------------------------------------------

// ConfigStatus is the convergence status of a single managed configuration.
type ConfigStatus string

const (
	ConfigConverged  ConfigStatus = "converged"
	ConfigConverging ConfigStatus = "converging"
	ConfigError      ConfigStatus = "error"
)

// ManagedConfig holds the full lifecycle state for one configuration.
type ManagedConfig struct {
	ID        ConfigID
	Desired   DesiredState
	Observed  ObservedState
	Recorded  RecordedState
	Graph     *Graph
	Status    ConfigStatus
	LastError string
	// DependsOnConfigs lists config names that must reach ConfigConverged before
	// this config's reconciliation loop begins.
	DependsOnConfigs []string
}
