// Package core defines the Converge execution engine and provider contract.
package core

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/akzj/converge/pkg/model"
)

// Provider owns all resource-specific planning and execution semantics.
type Provider interface {
	Type() string
	Digest() string
	Inspect(ctx context.Context, resource model.ResourceID) (model.ObservedState, error)
	Replan(ctx context.Context, request ReplanRequest) (ReplanResult, error)
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

// ReplanResult is provisional. Core assigns identity/generation, validates
// fingerprints and DAG structure, then installs it atomically.
type ReplanResult struct {
	Operations []model.Operation
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

// Journal records every Operation transition for audit and recovery.
type Journal interface {
	Append(ctx context.Context, event model.Event) error
	Events(ctx context.Context, configID string) ([]model.Event, error)
}

func log() *zap.Logger { return zap.L() }

func init() {
	logger, err := zap.NewProduction()
	if err != nil {
		panic(fmt.Sprintf("converge: init zap: %v", err))
	}
	zap.ReplaceGlobals(logger)
}
