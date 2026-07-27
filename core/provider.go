// Package core defines the Converge engine: reconciliation loop, DAG scheduler,
// executor, safety layer, and Provider contract.
package core

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/akzj/converge/pkg/model"
)

// Provider is the interface every Converge resource provider must implement.
//
// Converge Core does not interpret Action strings — they are opaque tokens
// passed from Diff to Execute. The Provider owns all resource-specific
// semantics: what "provision", "deprovision", or "reconcile" means for an
// nginx config vs a GPU driver vs a Vault cluster.
type Provider interface {
	// Type returns the resource type this provider handles (e.g. "nginx.config").
	Type() string

	// Inspect reads the current real-world state of a resource.
	Inspect(ctx context.Context, resource model.ResourceID) (model.ObservedState, error)

	// Diff computes the set of Operations needed to transform observed into desired.
	Diff(ctx context.Context, observed model.ObservedState, desired model.DesiredState) ([]model.Operation, error)

	// Execute performs one Operation and returns its result.
	Execute(ctx context.Context, op model.Operation) (model.StepResult, error)

	// Verify confirms convergence by reading back and comparing against desired.
	Verify(ctx context.Context, resource model.ResourceID, desired model.DesiredState) (model.ObservedState, error)
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

// log returns the package-level sugared logger.
func log() *zap.SugaredLogger {
	return zap.S()
}

func init() {
	logger, err := zap.NewProduction()
	if err != nil {
		panic(fmt.Sprintf("converge: init zap: %v", err))
	}
	zap.ReplaceGlobals(logger)
}
