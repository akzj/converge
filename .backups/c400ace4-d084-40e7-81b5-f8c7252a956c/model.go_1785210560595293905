// Package model defines the fundamental, language-neutral data types for
// the Converge reconciliation engine.
package model

import (
	"encoding/json"
	"time"
)

// ---------------------------------------------------------------------------
// Identity
// ---------------------------------------------------------------------------

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
	PhasePrepare    Phase = "prepare"    // side-effect-free preparation
	PhaseWait       Phase = "wait"       // waiting for a runtime condition
	PhaseCommit     Phase = "commit"     // destructive, potentially irreversible mutation
	PhaseVerify     Phase = "verify"     // read-back from real system
	PhaseCleanup    Phase = "cleanup"    // removal of artifacts owned by a superseded version
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
	Code        string    `json:"code,omitempty"`         // stable error code
	Reason      string    `json:"reason,omitempty"`       // human-readable explanation
	Retryable   bool      `json:"retryable"`              // true=may retry; false=wait for new desired
	NextCheckAt time.Time `json:"next_check_at,omitempty"`// when Waiting should be re-evaluated
}

// ---------------------------------------------------------------------------
// Condition
// ---------------------------------------------------------------------------

// Condition is a runtime guard evaluated before an Operation becomes ready.
type Condition struct {
	Name     string     `json:"name"`               // e.g. "no_active_players"
	Resource ResourceID `json:"resource,omitempty"`
	Input    []byte     `json:"input,omitempty"`    // condition-specific parameters
}

// ---------------------------------------------------------------------------
// CancelMode
// ---------------------------------------------------------------------------

// CancelMode expresses whether an in-flight Operation can be safely interrupted.
type CancelMode string

const (
	CancelModeSafe  CancelMode = "safe"   // cancellable at any point (download, inspect)
	CancelModeAsync CancelMode = "async"  // cancellable between sub-steps
	CancelModeNone  CancelMode = "none"   // not cancellable once started
)

// ---------------------------------------------------------------------------
// Operation
// ---------------------------------------------------------------------------

// Operation is the smallest atomic execution unit in Converge.
type Operation struct {
	ID          string      `json:"id"`                     // unique within the DAG
	ConfigID    string      `json:"config_id"`              // owning configuration name
	Provider    string      `json:"provider"`               // target provider name
	Action      string      `json:"action"`                 // "provision"|"deprovision"|"reconcile"
	Input       []byte      `json:"input,omitempty"`        // action-specific parameters (JSON)
	Phase       Phase       `json:"phase"`
	Destructive bool        `json:"destructive"`
	DependsOn   []string    `json:"depends_on,omitempty"`
	Conditions  []Condition `json:"conditions,omitempty"`
	Timeout     Duration    `json:"timeout,omitempty"`
	CancelMode  CancelMode  `json:"cancel_mode"`
	HandlerRef  string      `json:"handler_ref,omitempty"`
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
	ProviderType string   `json:"provider_type"`     // which provider handles this (e.g. "nginx.config")
	Version      uint64   `json:"version"`
	Spec         []byte   `json:"spec"`               // provider-specific desired spec (JSON)
	Digest       string   `json:"digest"`             // sha256 of Spec
	DependsOn    []string `json:"depends_on,omitempty"` // config names that must converge first
}

// ObservedState is what a Provider reads from the real system.
type ObservedState struct {
	ConfigID   ConfigID `json:"config_id"`
	Version    string   `json:"version,omitempty"`
	Properties []byte   `json:"properties,omitempty"`
	Digest     string   `json:"digest,omitempty"` // sha256 of Properties
	Present    bool     `json:"present"`           // false = resource does not exist
}

// ---------------------------------------------------------------------------
// RecordedState (State Store)
// ---------------------------------------------------------------------------

// RecordedState is persisted after each successful convergence.
type RecordedState struct {
	ConfigID       ConfigID  `json:"config_id"`
	ProviderType   string    `json:"provider_type"` // which provider owns this config
	DesiredVersion uint64    `json:"desired_version"`
	DesiredDigest  string    `json:"desired_digest"`
	HandlerDigest  string    `json:"handler_digest"`
	HandlerRef     string    `json:"handler_ref"`
	Status         string    `json:"status"` // "converged"|"converging"|"error"
	UpdatedAt      time.Time `json:"updated_at"`
}

// ---------------------------------------------------------------------------
// DAG
// ---------------------------------------------------------------------------

// NodeStatus tracks execution progress of an Operation within the DAG.
type NodeStatus string

const (
	NodePending   NodeStatus = "pending"
	NodeReady     NodeStatus = "ready"
	NodeRunning   NodeStatus = "running"
	NodeCompleted NodeStatus = "completed"
	NodeFailed    NodeStatus = "failed"
	NodeCancelled NodeStatus = "cancelled"
)

// Node wraps an Operation with its runtime status.
type Node struct {
	Operation Operation  `json:"operation"`
	Status    NodeStatus `json:"status"`
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

// ---------------------------------------------------------------------------
// Event
// ---------------------------------------------------------------------------

// Event is sent from Executor to Orchestrator after an Operation completes.
type Event struct {
	NodeID   string       `json:"node_id"`
	ConfigID string       `json:"config_id"`
	State    StepState    `json:"state"`
	Result   StepResult   `json:"result"`
	Observed ObservedState `json:"observed,omitempty"`
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
	ID       ConfigID
	Desired  DesiredState
	Observed ObservedState
	Recorded RecordedState
	Graph    *Graph
	Status   ConfigStatus
	// DependsOnConfigs lists config names that must reach ConfigConverged before
	// this config's reconciliation loop begins.
	DependsOnConfigs []string
}