package core

import (
	"fmt"
	"sort"

	"github.com/cockroachdb/errors"

	"github.com/akzj/converge/pkg/model"
)

// PlanChange partitions an old and candidate plan into mutually exclusive,
// exhaustive lifecycle actions.
type PlanChange struct {
	Carry  []model.OperationKey
	Drop   []model.OperationKey
	Cancel []model.OperationKey
	Drain  []model.OperationKey
	Add    []model.OperationKey
}

// BuildCandidate validates provider output, assigns semantic fingerprints, and
// creates an immutable plan candidate. Identity/generation are assigned by the
// atomic installer.
func BuildCandidate(config model.ConfigID, desired model.DesiredState, providerType, providerDigest string, operations []model.Operation) (*model.Plan, error) {
	plan := &model.Plan{
		ConfigID:       config,
		Desired:        model.CloneDesiredState(desired),
		DesiredVersion: desired.Version,
		DesiredDigest:  desired.Digest,
		ProviderType:   providerType,
		ProviderDigest: providerDigest,
		Nodes:          make(map[model.OperationKey]*model.Node, len(operations)),
	}
	for _, original := range operations {
		op := original
		if op.Provider != "" && op.Provider != providerType {
			return nil, errors.Errorf("operation %q provider %q does not match %q", op.Key, op.Provider, providerType)
		}
		op.Provider = providerType
		if op.ConflictKey == "" {
			op.ConflictKey = "config/" + config.Name
		}
		if op.Key == "" {
			return nil, errors.Errorf("operation %q has no stable key", op.ID)
		}
		if _, exists := plan.Nodes[op.Key]; exists {
			return nil, errors.Errorf("duplicate operation key %q", op.Key)
		}
		fingerprint, err := model.OperationFingerprint(op, providerDigest)
		if err != nil {
			return nil, errors.Wrapf(err, "fingerprint operation %q", op.Key)
		}
		if op.Fingerprint != "" && op.Fingerprint != fingerprint {
			return nil, errors.Errorf("provider fingerprint mismatch for %q", op.Key)
		}
		op.Fingerprint = fingerprint
		op.ConfigID = config.Name
		plan.Nodes[op.Key] = &model.Node{Operation: op, Status: model.NodePending}
	}
	if err := validatePlan(plan); err != nil {
		return nil, err
	}
	return plan, nil
}

func validatePlan(plan *model.Plan) error {
	if plan == nil {
		return errors.New("plan is nil")
	}
	graph := &model.Graph{Nodes: make(map[string]*model.Node, len(plan.Nodes))}
	for key, node := range plan.Nodes {
		if node == nil {
			return errors.Errorf("operation %q has nil node", key)
		}
		if node.Operation.Key != key {
			return errors.Errorf("operation map key %q does not match operation key %q", key, node.Operation.Key)
		}
		if node.Operation.Fingerprint == "" {
			return errors.Errorf("operation %q has empty fingerprint", key)
		}
		graph.Nodes[string(key)] = node
	}
	return graph.Validate()
}

// ClassifyPlanChange decides lifecycle actions without mutating either plan.
func ClassifyPlanChange(oldPlan, candidate *model.Plan) (PlanChange, error) {
	var change PlanChange
	if candidate == nil {
		return change, errors.New("candidate plan is nil")
	}
	if err := validatePlan(candidate); err != nil {
		return change, errors.Wrap(err, "invalid candidate plan")
	}
	if oldPlan == nil {
		for key := range candidate.Nodes {
			change.Add = append(change.Add, key)
		}
		sortChange(&change)
		return change, nil
	}
	if err := validatePlan(oldPlan); err != nil {
		return change, errors.Wrap(err, "invalid old plan")
	}

	for key, oldNode := range oldPlan.Nodes {
		_, exists := candidate.Nodes[key]
		if exists && carryCompatible(oldPlan, candidate, key, make(map[model.OperationKey]bool)) {
			change.Carry = append(change.Carry, key)
			continue
		}
		switch oldNode.Status {
		case model.NodePending, model.NodeReady, model.NodeWaiting, model.NodeCompleted, model.NodeFailed, model.NodeCancelled:
			change.Drop = append(change.Drop, key)
		case model.NodeRunning, model.NodeCancelling, model.NodeDraining:
			if oldNode.Operation.CancelMode == model.CancelModeNone {
				change.Drain = append(change.Drain, key)
			} else {
				change.Cancel = append(change.Cancel, key)
			}
		default:
			return PlanChange{}, errors.Errorf("operation %q has unknown status %q", key, oldNode.Status)
		}
	}
	for key := range candidate.Nodes {
		if !contains(change.Carry, key) {
			change.Add = append(change.Add, key)
		}
	}
	sortChange(&change)
	if err := validatePartition(oldPlan, candidate, change); err != nil {
		return PlanChange{}, err
	}
	return change, nil
}

func carryCompatible(oldPlan, candidate *model.Plan, key model.OperationKey, visiting map[model.OperationKey]bool) bool {
	oldNode, oldOK := oldPlan.Nodes[key]
	newNode, newOK := candidate.Nodes[key]
	if !oldOK || !newOK || oldNode == nil || newNode == nil || visiting[key] {
		return false
	}
	if oldPlan.ProviderType != candidate.ProviderType || oldPlan.ProviderDigest != candidate.ProviderDigest ||
		oldNode.Operation.Fingerprint == "" || oldNode.Operation.Fingerprint != newNode.Operation.Fingerprint {
		return false
	}
	switch oldNode.Status {
	case model.NodePending, model.NodeReady, model.NodeCompleted:
	case model.NodeRunning:
		if oldNode.AttemptID == "" {
			return false
		}
	default:
		return false
	}
	visiting[key] = true
	defer delete(visiting, key)
	for _, dependency := range oldNode.Operation.DependsOn {
		if !carryCompatible(oldPlan, candidate, model.OperationKey(dependency), visiting) {
			return false
		}
	}
	return true
}

func validatePartition(oldPlan, candidate *model.Plan, change PlanChange) error {
	oldCount := len(change.Carry) + len(change.Drop) + len(change.Cancel) + len(change.Drain)
	if oldCount != len(oldPlan.Nodes) {
		return errors.Errorf("old plan partition is incomplete: got %d, want %d", oldCount, len(oldPlan.Nodes))
	}
	if len(change.Carry)+len(change.Add) != len(candidate.Nodes) {
		return errors.Errorf("candidate partition is incomplete: got %d, want %d", len(change.Carry)+len(change.Add), len(candidate.Nodes))
	}
	seen := make(map[model.OperationKey]string)
	for label, keys := range map[string][]model.OperationKey{
		"carry": change.Carry, "drop": change.Drop, "cancel": change.Cancel, "drain": change.Drain,
	} {
		for _, key := range keys {
			if prior, ok := seen[key]; ok {
				return fmt.Errorf("operation %q appears in both %s and %s", key, prior, label)
			}
			seen[key] = label
		}
	}
	return nil
}

func sortChange(change *PlanChange) {
	less := func(keys []model.OperationKey) { sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] }) }
	less(change.Carry)
	less(change.Drop)
	less(change.Cancel)
	less(change.Drain)
	less(change.Add)
}

func contains(keys []model.OperationKey, target model.OperationKey) bool {
	for _, key := range keys {
		if key == target {
			return true
		}
	}
	return false
}
