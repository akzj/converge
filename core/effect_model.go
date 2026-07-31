package core

import (
	"encoding/json"
	"time"

	"github.com/akzj/converge/pkg/model"
)

type EffectID string
type ReferenceID string
type PollRequestID string
type ControlRequestID string

type EffectBindingState string

const (
	EffectBindingUnbound EffectBindingState = "unbound"
	EffectBindingBound   EffectBindingState = "bound"
)

type ExternalEffectState string

const (
	ExternalEffectEnsuring        ExternalEffectState = "ensuring"
	ExternalEffectActive          ExternalEffectState = "active"
	ExternalEffectCancelRequested ExternalEffectState = "cancel_requested"
	ExternalEffectCancelling      ExternalEffectState = "cancelling"
	ExternalEffectCompleted       ExternalEffectState = "completed"
	ExternalEffectCancelled       ExternalEffectState = "cancelled"
	ExternalEffectFailed          ExternalEffectState = "failed"
	ExternalEffectUnknown         ExternalEffectState = "unknown"
)

type EffectReferenceState string

const (
	EffectReferenceEnsuring         EffectReferenceState = "ensuring"
	EffectReferenceActive           EffectReferenceState = "active"
	EffectReferenceReleaseRequested EffectReferenceState = "release_requested"
	EffectReferenceReleased         EffectReferenceState = "released"
)

type EffectControlKind string

const (
	EffectControlEnsureRetry         EffectControlKind = "ensure_retry"
	EffectControlEnsureReference     EffectControlKind = "ensure_reference"
	EffectControlObserve             EffectControlKind = "observe"
	EffectControlRelease             EffectControlKind = "release"
	EffectControlObserveCancellation EffectControlKind = "observe_cancellation"
)

// EffectTargetKind distinguishes controls that drive a plan DAG node
// (PlanNode) from maintenance controls that outlive plan-node lifecycle
// (Maintenance, e.g. deletion releases). PlanNode controls MUST carry complete
// NodeIdentity; Maintenance controls MUST NOT carry a dangling NodeIdentity.
type EffectTargetKind string

const (
	EffectTargetPlanNode    EffectTargetKind = "plan_node"
	EffectTargetMaintenance EffectTargetKind = "maintenance"
)

type EffectControlState string

const (
	EffectControlPending   EffectControlState = "pending"
	EffectControlInFlight  EffectControlState = "in_flight"
	EffectControlYielded   EffectControlState = "yielded"
	EffectControlCompleted EffectControlState = "completed"
)

type ActiveEffect struct {
	ID                  EffectID            `json:"id"`
	Binding             EffectBindingState  `json:"binding"`
	ExternalJobID       string              `json:"external_job_id,omitempty"`
	ArtifactID          string              `json:"artifact_id"`
	IdempotencyKey      string              `json:"idempotency_key"`
	SemanticFingerprint string              `json:"semantic_fingerprint"`
	EnsureSpec          json.RawMessage     `json:"ensure_spec"`
	ProviderType        string              `json:"provider_type"`
	ProviderDigest      string              `json:"provider_digest"`
	ConflictKey         string              `json:"conflict_key"`
	State               ExternalEffectState `json:"state"`
	ResolutionRequired  bool                `json:"resolution_required"`
	ExternalRevision    uint64              `json:"external_revision"`
	CancelEpoch         uint64              `json:"cancel_epoch"`
	UpdatedAt           time.Time           `json:"updated_at"`
}

type EffectReference struct {
	ID         ReferenceID          `json:"id"`
	EffectID   EffectID             `json:"effect_id"`
	ConfigID   model.ConfigID       `json:"config_id"`
	PlanID     model.PlanID         `json:"plan_id"`
	Generation model.Generation     `json:"generation"`
	EffectKey  string               `json:"effect_key"`
	State      EffectReferenceState `json:"state"`
}

type EffectControl struct {
	ID                ControlRequestID   `json:"id"`
	ConfigID          model.ConfigID     `json:"config_id"`
	ProviderType      string             `json:"provider_type"`
	ProviderDigest    string             `json:"provider_digest"`
	Kind              EffectControlKind  `json:"kind"`
	TargetKind        EffectTargetKind   `json:"target_kind,omitempty"`
	State             EffectControlState `json:"state"`
	EffectID          EffectID           `json:"effect_id"`
	ReferenceID       ReferenceID        `json:"reference_id"`
	PlanID            model.PlanID       `json:"plan_id,omitempty"`
	Generation        model.Generation   `json:"generation,omitempty"`
	OperationKey      model.OperationKey `json:"operation_key,omitempty"`
	NextCheckAt       time.Time          `json:"next_check_at"`
	RetryCount        uint32             `json:"retry_count"`
	InFlightAttemptID model.AttemptID    `json:"in_flight_attempt_id,omitempty"`
	PollRequestID     PollRequestID      `json:"poll_request_id,omitempty"`
	LeaseExpiresAt    time.Time          `json:"lease_expires_at,omitempty"`
}

func (e ActiveEffect) Clone() ActiveEffect {
	e.EnsureSpec = append(json.RawMessage(nil), e.EnsureSpec...)
	return e
}
