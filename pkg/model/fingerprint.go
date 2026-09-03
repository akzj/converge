package model

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"slices"

	"github.com/cockroachdb/errors"
)

// OperationFingerprint returns a deterministic semantic fingerprint for op.
// Runtime identity and state (ID, Key, ConfigID, Fingerprint) are deliberately
// excluded. Provider digest is included so implementations are never reused
// across an upgrade without explicit replanning.
func OperationFingerprint(op Operation, providerDigest string) (string, error) {
	input, err := canonicalJSON(op.Input)
	if err != nil {
		return "", errors.Wrap(err, "canonicalize operation input")
	}

	deps := slices.Clone(op.DependsOn)
	slices.Sort(deps)

	conditions := make([]fingerprintCondition, 0, len(op.Conditions))
	for _, condition := range op.Conditions {
		conditionInput, err := canonicalJSON(condition.Input)
		if err != nil {
			return "", errors.Wrapf(err, "canonicalize condition %q input", condition.Name)
		}
		conditions = append(conditions, fingerprintCondition{
			Name:     condition.Name,
			Resource: condition.Resource,
			Input:    conditionInput,
		})
	}
	slices.SortFunc(conditions, func(a, b fingerprintCondition) int {
		ab, _ := json.Marshal(a)
		bb, _ := json.Marshal(b)
		return slices.Compare(ab, bb)
	})

	canonical := fingerprintOperation{
		Provider:        op.Provider,
		ExecutionKind:   op.ExecutionKind,
		EffectKey:       op.EffectKey,
		TargetReference: op.TargetReference,
		ReleaseTarget:   op.ReleaseTarget,
		ProviderDigest:  providerDigest,
		Action:          op.Action,
		Input:           input,
		Phase:           op.Phase,
		Destructive:     op.Destructive,
		DependsOn:       deps,
		Conditions:      conditions,
		Timeout:         op.Timeout,
		CancelMode:      op.CancelMode,
		HandlerRef:      op.HandlerRef,
		ConflictKey:     op.ConflictKey,
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return "", errors.Wrap(err, "marshal fingerprint input")
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:]), nil
}

// DesiredSpecDigest returns the wire-format digest used by DesiredState.
// Desired specs are hashed byte-for-byte: providers remain responsible for
// defining whether semantically equivalent JSON should be normalized upstream.
func DesiredSpecDigest(spec []byte) string {
	digest := sha256.Sum256(spec)
	return "sha256:" + hex.EncodeToString(digest[:])
}

type fingerprintOperation struct {
	Provider        string                 `json:"provider"`
	ExecutionKind   OperationExecutionKind `json:"execution_kind"`
	EffectKey       string                 `json:"effect_key"`
	TargetReference string                 `json:"target_reference"`
	ReleaseTarget   ReleaseTargetKind      `json:"release_target"`
	ProviderDigest  string                 `json:"provider_digest"`
	Action          string                 `json:"action"`
	Input           json.RawMessage        `json:"input"`
	Phase           Phase                  `json:"phase"`
	Destructive     bool                   `json:"destructive"`
	DependsOn       []string               `json:"depends_on"`
	Conditions      []fingerprintCondition `json:"conditions"`
	Timeout         Duration               `json:"timeout"`
	CancelMode      CancelMode             `json:"cancel_mode"`
	HandlerRef      string                 `json:"handler_ref"`
	ConflictKey     string                 `json:"conflict_key"`
}

type fingerprintCondition struct {
	Name     string          `json:"name"`
	Resource ResourceID      `json:"resource"`
	Input    json.RawMessage `json:"input"`
}

func canonicalJSON(input []byte) (json.RawMessage, error) {
	if len(input) == 0 {
		return json.RawMessage("null"), nil
	}
	var value any
	decoder := json.NewDecoder(bytes.NewReader(input))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	// Reject a valid JSON value followed by non-whitespace data.
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return nil, errors.New("multiple JSON values")
		}
		return nil, err
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return encoded, nil
}
