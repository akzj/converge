package model

import "time"

// Plan is an immutable, validated execution plan for one desired revision.
type Plan struct {
	ID             PlanID                 `json:"id"`
	ConfigID       ConfigID               `json:"config_id"`
	DesiredVersion uint64                 `json:"desired_version"`
	DesiredDigest  string                 `json:"desired_digest"`
	ProviderType   string                 `json:"provider_type"`
	ProviderDigest string                 `json:"provider_digest"`
	Generation     Generation             `json:"generation"`
	Nodes          map[OperationKey]*Node `json:"nodes"`
}

// AttemptStatus is the lifecycle of one real execution attempt.
type AttemptStatus string

const (
	AttemptPending    AttemptStatus = "pending"
	AttemptRunning    AttemptStatus = "running"
	AttemptWaiting    AttemptStatus = "waiting"
	AttemptCancelling AttemptStatus = "cancelling"
	AttemptDraining   AttemptStatus = "draining"
	AttemptCompleted  AttemptStatus = "completed"
	AttemptFailed     AttemptStatus = "failed"
	AttemptCancelled  AttemptStatus = "cancelled"
	AttemptUnknown    AttemptStatus = "unknown"
)

// Attempt tracks one actual provider execution independently from plan nodes.
type Attempt struct {
	ID          AttemptID     `json:"id"`
	PlanID      PlanID        `json:"plan_id"`
	Generation  Generation    `json:"generation"`
	ConfigID    ConfigID      `json:"config_id"`
	NodeKey     OperationKey  `json:"node_key"`
	Fingerprint string        `json:"fingerprint"`
	ConflictKey string        `json:"conflict_key"`
	Status      AttemptStatus `json:"status"`
	CarriedTo   Generation    `json:"carried_to,omitempty"`
	StartedAt   time.Time     `json:"started_at,omitempty"`
	UpdatedAt   time.Time     `json:"updated_at,omitempty"`
	NextCheckAt time.Time     `json:"next_check_at,omitempty"`
}

// PlanSnapshot is a deep-copy view passed to providers during replanning.
type PlanSnapshot struct {
	Plan     *Plan     `json:"plan,omitempty"`
	Attempts []Attempt `json:"attempts,omitempty"`
}

// Clone returns a provider-safe deep copy.
func (p *Plan) Clone() *Plan {
	if p == nil {
		return nil
	}
	clone := *p
	clone.Nodes = make(map[OperationKey]*Node, len(p.Nodes))
	for key, node := range p.Nodes {
		if node == nil {
			clone.Nodes[key] = nil
			continue
		}
		nodeClone := *node
		nodeClone.Operation.Input = append([]byte(nil), node.Operation.Input...)
		nodeClone.Operation.DependsOn = append([]string(nil), node.Operation.DependsOn...)
		nodeClone.Operation.Conditions = append([]Condition(nil), node.Operation.Conditions...)
		for i := range nodeClone.Operation.Conditions {
			nodeClone.Operation.Conditions[i].Input = append([]byte(nil), node.Operation.Conditions[i].Input...)
		}
		clone.Nodes[key] = &nodeClone
	}
	return &clone
}
