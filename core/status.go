package core

import (
	"slices"
	"strings"
	"time"

	"github.com/akzj/converge/pkg/model"
)

// ConfigReport is a payload-redacted operational view suitable for an Edge
// status API. It reports durable execution identity and current convergence.
type ConfigReport struct {
	ConfigID       model.ConfigID     `json:"config_id"`
	ProviderType   string             `json:"provider_type,omitempty"`
	DesiredVersion uint64             `json:"desired_version,omitempty"`
	DesiredDigest  string             `json:"desired_digest,omitempty"`
	Status         model.ConfigStatus `json:"status"`
	LastError      string             `json:"last_error,omitempty"`
	Observed       ObservedReport     `json:"observed"`
	PlanID         model.PlanID       `json:"plan_id,omitempty"`
	Generation     model.Generation   `json:"generation,omitempty"`
	Deleting       bool               `json:"deleting,omitempty"`
	Nodes          []NodeReport       `json:"nodes,omitempty"`
	Attempts       []AttemptReport    `json:"attempts,omitempty"`
	Effects        []EffectReport     `json:"effects,omitempty"`
	Controls       []ControlReport    `json:"controls,omitempty"`
}

type ObservedReport struct {
	Version string `json:"version,omitempty"`
	Digest  string `json:"digest,omitempty"`
	Present bool   `json:"present"`
}

type NodeReport struct {
	Key        model.OperationKey           `json:"key"`
	Kind       model.OperationExecutionKind `json:"kind"`
	Status     model.NodeStatus             `json:"status"`
	AttemptID  model.AttemptID              `json:"attempt_id,omitempty"`
	RetryCount int                          `json:"retry_count,omitempty"`
}

type AttemptReport struct {
	ID         model.AttemptID     `json:"id"`
	NodeKey    model.OperationKey  `json:"node_key"`
	Status     model.AttemptStatus `json:"status"`
	Generation model.Generation    `json:"generation"`
	UpdatedAt  time.Time           `json:"updated_at,omitempty"`
}

type EffectReport struct {
	ID                 EffectID            `json:"id"`
	State              ExternalEffectState `json:"state"`
	Binding            EffectBindingState  `json:"binding"`
	ExternalRevision   uint64              `json:"external_revision,omitempty"`
	ResolutionRequired bool                `json:"resolution_required"`
}

type ControlReport struct {
	ID             ControlRequestID   `json:"id"`
	Kind           EffectControlKind  `json:"kind"`
	State          EffectControlState `json:"state"`
	TargetKind     EffectTargetKind   `json:"target_kind"`
	RetryCount     uint32             `json:"retry_count,omitempty"`
	NextCheckAt    time.Time          `json:"next_check_at,omitempty"`
	LeaseExpiresAt time.Time          `json:"lease_expires_at,omitempty"`
}

func (r *Reconciler) Report(name string) (ConfigReport, bool) {
	managed, ok := r.Config(name)
	if !ok {
		return ConfigReport{}, false
	}
	execution := r.registry.Execution(managed.ID)
	report := ConfigReport{
		ConfigID: managed.ID, ProviderType: managed.Desired.ProviderType,
		DesiredVersion: managed.Desired.Version, DesiredDigest: managed.Desired.Digest,
		Status: managed.Status, LastError: managed.LastError,
		Observed: ObservedReport{Version: managed.Observed.Version, Digest: managed.Observed.Digest, Present: managed.Observed.Present},
		Deleting: execution.Deleting,
	}
	if execution.Plan != nil {
		report.PlanID = execution.Plan.ID
		report.Generation = execution.Plan.Generation
		for key, node := range execution.Plan.Nodes {
			report.Nodes = append(report.Nodes, NodeReport{Key: key, Kind: node.Operation.ExecutionKind, Status: node.Status, AttemptID: node.AttemptID, RetryCount: node.RetryCount})
		}
	}
	for _, attempt := range execution.Attempts {
		report.Attempts = append(report.Attempts, AttemptReport{ID: attempt.ID, NodeKey: attempt.NodeKey, Status: attempt.Status, Generation: attempt.Generation, UpdatedAt: attempt.UpdatedAt})
	}
	for _, effect := range execution.Effects {
		report.Effects = append(report.Effects, EffectReport{ID: effect.ID, State: effect.State, Binding: effect.Binding, ExternalRevision: effect.ExternalRevision, ResolutionRequired: effect.ResolutionRequired})
	}
	for _, control := range execution.EffectControls {
		report.Controls = append(report.Controls, ControlReport{ID: control.ID, Kind: control.Kind, State: control.State, TargetKind: control.TargetKind, RetryCount: control.RetryCount, NextCheckAt: control.NextCheckAt, LeaseExpiresAt: control.LeaseExpiresAt})
	}
	slices.SortFunc(report.Nodes, func(a, b NodeReport) int { return strings.Compare(string(a.Key), string(b.Key)) })
	slices.SortFunc(report.Attempts, func(a, b AttemptReport) int { return strings.Compare(string(a.ID), string(b.ID)) })
	slices.SortFunc(report.Effects, func(a, b EffectReport) int { return strings.Compare(string(a.ID), string(b.ID)) })
	slices.SortFunc(report.Controls, func(a, b ControlReport) int { return strings.Compare(string(a.ID), string(b.ID)) })
	return report, true
}

func (r *Reconciler) Reports() []ConfigReport {
	names := r.ConfigNames()
	reports := make([]ConfigReport, 0, len(names))
	for _, name := range names {
		if report, ok := r.Report(name); ok {
			reports = append(reports, report)
		}
	}
	return reports
}
