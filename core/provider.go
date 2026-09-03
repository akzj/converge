// Package core defines the Converge execution engine and provider contract.
package core

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/akzj/converge/pkg/model"
)

var ErrDesiredSnapshotConflict = errors.New("desired snapshot revision conflict")

// Provider owns all resource-specific planning and execution semantics.
type Provider interface {
	Type() string
	Digest() string
	Inspect(ctx context.Context, resource model.ResourceID) (model.ObservedState, error)
	Replan(ctx context.Context, request ReplanRequest) (ReplanResult, error)
	EvaluateCondition(ctx context.Context, condition model.Condition) (bool, error)
	Execute(ctx context.Context, op model.Operation) (model.StepResult, error)
	Verify(ctx context.Context, resource model.ResourceID, desired model.DesiredState) (model.ObservedState, error)
}

// ReplanRequest is the provider's read-only input for building a candidate plan.
type ReplanRequest struct {
	Observed       model.ObservedState
	Desired        model.DesiredState
	Active         model.PlanSnapshot
	ProviderDigest string
}

// EffectResolution is a provider's authoritative assessment of an Unknown
// attempt after inspecting real state.
type EffectResolution string

const (
	EffectStillActive EffectResolution = "still_active"
	EffectCompleted   EffectResolution = "completed"
	EffectAbsent      EffectResolution = "absent"
)

// ReplanResult is provisional. Core assigns identity/generation, validates
// fingerprints and DAG structure, then installs it atomically.
type ReplanResult struct {
	Operations  []model.Operation
	Resolutions map[model.AttemptID]EffectResolution
}

// StateStore persists the last-known applied state for each configuration.
type StateStore interface {
	Get(ctx context.Context, configID model.ConfigID) (*model.RecordedState, error)
	List(ctx context.Context) ([]model.ConfigID, error)
	Record(ctx context.Context, state model.RecordedState) error
	Delete(ctx context.Context, configID model.ConfigID) error
}

// EventBus delivers Operation completion events from Executor to Orchestrator.
type EventBus interface {
	Publish(ctx context.Context, event model.Event) error
	Subscribe(ctx context.Context, configID string) (<-chan model.Event, error)
}

// Arbiter enforces mutual exclusion for destructive operations.
type Arbiter interface {
	Acquire(ctx context.Context, operationID string) (release func(), err error)
}

// ExecutionSnapshot is the durable execution state for one configuration.
type ExecutionSnapshot struct {
	Revision         uint64
	Deleting         bool
	AcceptedDesired  *model.DesiredState
	Plan             *model.Plan
	Attempts         []model.Attempt
	Outbox           []model.Event
	Effects          []ActiveEffect
	EffectReferences []EffectReference
	EffectControls   []EffectControl
}

// ExecutionStore persists plan/attempt transitions used for crash recovery.
type ExecutionStore interface {
	LoadExecution(ctx context.Context, configID model.ConfigID) (*ExecutionSnapshot, error)
	ListExecutions(ctx context.Context) ([]model.ConfigID, error)
	CommitExecutionCAS(ctx context.Context, configID model.ConfigID, expectedRevision uint64, snapshot ExecutionSnapshot) error
	DeleteExecution(ctx context.Context, configID model.ConfigID) error
}

// DesiredSnapshotStore durably accepts the latest complete server snapshot.
// A successful accepted result is the durable ACK boundary; reconciliation is
// intentionally asynchronous.
type DesiredSnapshotStore interface {
	AcceptDesiredSnapshot(ctx context.Context, snapshot model.DesiredSnapshot) (accepted bool, err error)
	LoadDesiredSnapshot(ctx context.Context) (*model.DesiredSnapshot, error)
}

// Journal records every Operation transition for audit and recovery.
type Journal interface {
	Append(ctx context.Context, event model.Event) error
	Events(ctx context.Context, configID string) ([]model.Event, error)
}
