package core

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/akzj/converge/pkg/model"
)

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
