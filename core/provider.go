// Package core defines the Converge engine: reconciliation loop, DAG scheduler,
// executor, safety layer, and Provider contract.
package core

import (
	"context"

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
//
// Without StateStore, Converge cannot track what was previously applied,
// detect drift, or recover after restart.
type StateStore interface {
	// Get retrieves the recorded state for a configuration.
	Get(ctx context.Context, configID model.ConfigID) (*model.RecordedState, error)

	// List returns all configuration names currently tracked in the store.
	List(ctx context.Context) ([]model.ConfigID, error)

	// Record saves the applied state after a successful convergence.
	Record(ctx context.Context, state model.RecordedState) error

	// Delete removes a configuration from the store (after it has been deprovisioned).
	Delete(ctx context.Context, configID model.ConfigID) error
}

// EventBus delivers Operation completion events from Executor to Orchestrator.
type EventBus interface {
	// Publish sends an event.
	Publish(ctx context.Context, event model.Event) error

	// Subscribe returns a channel that receives events for the given configuration.
	// If configID is empty, receives events for all configurations.
	Subscribe(ctx context.Context, configID string) (<-chan model.Event, error)
}

// Arbiter enforces mutual exclusion for destructive operations.
// At most one destructive Commit may run across all configurations.
type Arbiter interface {
	// Acquire attempts to reserve the host-level destructive lease.
	// Returns a release function. Returns error if another destructive op holds the lease.
	Acquire(ctx context.Context, operationID string) (release func(), err error)
}

// Journal records every Operation transition for audit and recovery.
type Journal interface {
	Append(ctx context.Context, event model.Event) error
	Events(ctx context.Context, configID string) ([]model.Event, error)
}