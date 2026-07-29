package core

import (
	"context"
	"encoding/json"
	"time"

	"github.com/akzj/converge/pkg/model"
)

// EffectProvider extends Provider with explicit external-effect control methods.
// Core uses typed commands rather than overloading Execution.Action parsing.
type EffectProvider interface {
	Provider
	EnsureEffect(context.Context, EnsureEffectRequest) (EnsureEffectResult, error)
	ObserveEffects(context.Context, []ObserveEffectRequest) (map[PollRequestID]EffectObservationResult, error)
	EnsureReference(context.Context, EnsureReferenceRequest) (EnsureReferenceResult, error)
	ReleaseEffect(context.Context, ReleaseEffectRequest) (ReleaseEffectResult, error)
}

type EnsureEffectRequest struct {
	Identity            EffectIdentity
	IdempotencyKey      string
	ArtifactID          string
	SemanticFingerprint string
	EnsureSpec          json.RawMessage
}

type EnsureEffectResult struct {
	EffectID         EffectID
	ReferenceID      ReferenceID
	ExternalJobID    string
	ExternalRevision uint64
	Disposition      EnsureDisposition
	Failure          EnsureFailureKind
	Code             string
	Reason           string
}

type ObserveEffectRequest struct {
	Identity         EffectIdentity
	AttemptID        model.AttemptID
	PollRequestID    PollRequestID
	ExternalJobID    string
	ExternalRevision uint64
}

type EffectObservation struct {
	EffectID         EffectID
	AttemptID        model.AttemptID
	PollRequestID    PollRequestID
	ExternalJobID    string
	ExternalRevision uint64
	Disposition      EffectDisposition
	Retryable        bool
	Code, Reason     string
	NextCheckAt      time.Time
}

type EffectObservationResult struct {
	Observation *EffectObservation
	Error       *ProviderEffectError
}

type EnsureReferenceRequest struct {
	Identity         EffectIdentity
	RequestID        ControlRequestID
	ExternalJobID    string
	ExternalRevision uint64
}

type EnsureReferenceResult struct {
	EffectID         EffectID
	ReferenceID      ReferenceID
	RequestID        ControlRequestID
	ExternalJobID    string
	ExternalRevision uint64
	Disposition      EnsureDisposition
	Failure          EnsureFailureKind
	Code             string
	Reason           string
}

type ReleaseEffectRequest struct {
	Identity         EffectIdentity
	ReleaseRequestID ControlRequestID
	ExternalJobID    string
	ExternalRevision uint64
}

type ReleaseEffectResult struct {
	EffectID         EffectID
	ReferenceID      ReferenceID
	ReleaseRequestID ControlRequestID
	ExternalJobID    string
	ExternalRevision uint64
	Disposition      ReleaseDisposition
	Failure          ReleaseFailureKind
	Code             string
	Reason           string
}

type ProviderEffectError struct {
	Code      string
	Reason    string
	Retryable bool
}

type EffectIdentity struct {
	EffectID       EffectID
	ReferenceID    ReferenceID
	ConfigID       model.ConfigID
	PlanID         model.PlanID
	Generation     model.Generation
	OperationKey   model.OperationKey
	EffectKey      string
	ProviderType   string
	ProviderDigest string
}

type EnsureDisposition string

const (
	EnsureBound   EnsureDisposition = "bound"
	EnsureUnknown EnsureDisposition = "unknown"
	EnsureFailed  EnsureDisposition = "failed"
)

type EnsureFailureKind string

type TransitionDisposition string

const (
	TransitionApplied   TransitionDisposition = "applied"
	TransitionDuplicate TransitionDisposition = "duplicate"
	TransitionStale     TransitionDisposition = "stale"
	TransitionRejected  TransitionDisposition = "rejected"
)

const (
	EnsureFailureNone                     EnsureFailureKind = "none"
	EnsureFailureTransientKnownNotApplied EnsureFailureKind = "transient_known_not_applied"
	EnsureFailureUnknownOutcome           EnsureFailureKind = "unknown_outcome"
	EnsureFailureAuthoritativeRejected    EnsureFailureKind = "authoritative_rejected"
)

type EffectDisposition string

const (
	DispositionStillActive EffectDisposition = "still_active"
	DispositionCompleted   EffectDisposition = "completed"
	DispositionAbsent      EffectDisposition = "absent"
	DispositionCancelled   EffectDisposition = "cancelled"
	DispositionFailed      EffectDisposition = "failed"
)

type ReleaseDisposition string

const (
	ReleaseStillReferenced              ReleaseDisposition = "still_referenced"
	ReleaseLastReferenceCancelRequested ReleaseDisposition = "last_reference_cancel_requested"
	ReleaseConfirmed                    ReleaseDisposition = "released"
	ReleaseUnknown                      ReleaseDisposition = "unknown"
	ReleaseFailed                       ReleaseDisposition = "failed"
)

type ReleaseFailureKind string

const (
	ReleaseFailureNone           ReleaseFailureKind = "none"
	ReleaseFailureTransient      ReleaseFailureKind = "transient"
	ReleaseFailureUnknownOutcome ReleaseFailureKind = "unknown_outcome"
	ReleaseFailurePermanent      ReleaseFailureKind = "permanent"
)
