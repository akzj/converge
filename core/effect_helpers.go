package core

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/akzj/converge/pkg/model"
)

// finishEffectNodeBundleLocked completes the plan node, retires the control
// attempt, enqueues the lifecycle outbox event, and persists in one CAS.
// Caller holds the registry write lock and has already mutated effect/
// reference/control state on the clone. The node becomes Failed when the event
// carries StepFailed.
func (r *PlanRegistry) finishEffectNodeBundleLocked(ctx context.Context, state *configExecution, identity TransitionIdentity, nodeKey model.OperationKey, event model.Event) error {
	if nodeKey == "" || nodeKey != identity.EffectIdentity.OperationKey {
		return errors.New("terminal bundle node key does not match control identity")
	}
	node := state.active.Nodes[nodeKey]
	if node == nil {
		return errors.Errorf("operation %q not found", nodeKey)
	}
	if node.Status != model.NodeCompleted {
		attempt := &model.Attempt{
			ID: identity.AttemptID, PlanID: state.active.ID, Generation: state.active.Generation,
			ConfigID: identity.EffectIdentity.ConfigID, NodeKey: nodeKey,
			Fingerprint: node.Operation.Fingerprint, ConflictKey: node.Operation.ConflictKey,
			Status: model.AttemptCompleted,
		}
		if event.State == model.StepFailed {
			attempt.Status = model.AttemptFailed
			node.Status = model.NodeFailed
		} else {
			node.Status = model.NodeCompleted
		}
		node.AttemptID = identity.AttemptID
		state.retired[identity.AttemptID] = attempt
		delete(state.attempts, identity.AttemptID)
	}
	if event.EventID == "" {
		return errors.New("outbox event ID is empty")
	}
	state.outbox[event.EventID] = event
	return r.persistLocked(ctx, identity.EffectIdentity.ConfigID, state.revision, state)
}

// verifyControlNodeIdentity rejects incomplete identities and returns stale if
// the complete identity does not match the active plan. Callers invoke it
// before mutating any state.
func verifyControlNodeIdentity(identity TransitionIdentity, active *model.Plan) TransitionDisposition {
	if active == nil {
		return TransitionRejected
	}
	if identity.EffectIdentity.PlanID == "" || identity.EffectIdentity.Generation == 0 || identity.EffectIdentity.OperationKey == "" {
		return TransitionRejected
	}
	if identity.EffectIdentity.PlanID != active.ID {
		return TransitionStale
	}
	if identity.EffectIdentity.Generation != active.Generation {
		return TransitionStale
	}
	if active.Nodes[identity.EffectIdentity.OperationKey] == nil {
		return TransitionStale
	}
	return TransitionApplied
}

// retireControlAttemptLocked moves a claimed control-poll Attempt from the
// active attempts map to retired with the given terminal status. Caller holds
// the registry write lock.
func retireControlAttemptLocked(state *configExecution, attemptID model.AttemptID, status model.AttemptStatus) {
	if attemptID == "" {
		return
	}
	if attempt, ok := state.attempts[attemptID]; ok {
		attempt.Status = status
		attempt.UpdatedAt = time.Now()
		state.retired[attemptID] = attempt
		delete(state.attempts, attemptID)
	}
}

func newEffectID() (EffectID, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return EffectID("eff-" + hex.EncodeToString(value[:])), nil
}

func newReferenceID(configID model.ConfigID, planID model.PlanID, generation model.Generation, effectKey string) ReferenceID {
	return ReferenceID(fmt.Sprintf("%s/%s/%d/%s", configID.Name, string(planID), uint64(generation), effectKey))
}

// findEffectOperationKey returns the operation key of the plan node that
// matches the given EffectKey and ExecutionKind, or empty if not found.
func findEffectOperationKey(plan *model.Plan, effectKey string, kind model.OperationExecutionKind) model.OperationKey {
	if plan == nil {
		return ""
	}
	for key, node := range plan.Nodes {
		op := node.Operation
		if op.EffectKey == effectKey && op.ExecutionKind == kind {
			return key
		}
	}
	return ""
}

// bindControlToPlanNodeOrMaintenance makes the control target explicit. A
// provider lifecycle action that has no node in the current plan is maintenance
// work; it must not retain a partial identity that could advance a DAG node.
func bindControlToPlanNodeOrMaintenance(control *EffectControl, plan *model.Plan, effectKey string, kind model.OperationExecutionKind) {
	key := findEffectOperationKey(plan, effectKey, kind)
	if plan == nil || key == "" {
		control.TargetKind = EffectTargetMaintenance
		control.PlanID = ""
		control.Generation = 0
		control.OperationKey = ""
		return
	}
	control.TargetKind = EffectTargetPlanNode
	control.PlanID = plan.ID
	control.Generation = plan.Generation
	control.OperationKey = key
}
