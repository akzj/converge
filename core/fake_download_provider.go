package core

import (
	"context"
	"fmt"

	"github.com/akzj/converge/pkg/model"
)

// FakeDownloadProvider implements EffectProvider by delegating to a
// FakeDownloadService. It is a testing adapter, not a production provider.
type FakeDownloadProvider struct {
	digest       string
	service      *FakeDownloadService
	ensureCount  int
	observeCount int
	releaseCount int
}

func NewFakeDownloadProvider(digest string, service *FakeDownloadService) *FakeDownloadProvider {
	return &FakeDownloadProvider{digest: digest, service: service}
}

func (p *FakeDownloadProvider) Type() string   { return "fake_download" }
func (p *FakeDownloadProvider) Digest() string { return p.digest }
func (p *FakeDownloadProvider) Inspect(_ context.Context, _ model.ResourceID) (model.ObservedState, error) {
	return model.ObservedState{Present: false}, nil
}
func (p *FakeDownloadProvider) EvaluateCondition(_ context.Context, _ model.Condition) (bool, error) {
	return true, nil
}
func (p *FakeDownloadProvider) Verify(_ context.Context, _ model.ResourceID, _ model.DesiredState) (model.ObservedState, error) {
	return model.ObservedState{Present: true}, nil
}
func (p *FakeDownloadProvider) Replan(_ context.Context, _ ReplanRequest) (ReplanResult, error) {
	return ReplanResult{}, fmt.Errorf("FakeDownloadProvider: Replan not implemented for direct use")
}
func (p *FakeDownloadProvider) Execute(_ context.Context, _ model.Operation) (model.StepResult, error) {
	return model.StepResult{State: model.StepCompleted}, nil
}

func (p *FakeDownloadProvider) EnsureEffect(_ context.Context, req EnsureEffectRequest) (EnsureEffectResult, error) {
	p.ensureCount++
	jobID, revision, _ := p.service.CreateOrGetJobAndEnsureReference(req.IdempotencyKey, req.ArtifactID, string(req.Identity.ReferenceID))
	return EnsureEffectResult{
		EffectID:         req.Identity.EffectID,
		ReferenceID:      req.Identity.ReferenceID,
		ExternalJobID:    jobID,
		ExternalRevision: revision,
		Disposition:      EnsureBound,
		Failure:          EnsureFailureNone,
	}, nil
}

func (p *FakeDownloadProvider) ObserveEffects(_ context.Context, requests []ObserveEffectRequest) (map[PollRequestID]EffectObservationResult, error) {
	result := make(map[PollRequestID]EffectObservationResult, len(requests))
	for _, req := range requests {
		p.observeCount++
		state, revision, refs, err := p.service.GetJob(req.ExternalJobID)
		if err != nil {
			result[req.PollRequestID] = EffectObservationResult{
				Error: &ProviderEffectError{Code: "job_not_found", Reason: err.Error(), Retryable: true},
			}
			continue
		}
		disposition, retryable, code, reason := jobDisposition(state)
		if len(refs) == 0 {
			result[req.PollRequestID] = EffectObservationResult{
				Observation: &EffectObservation{
					EffectID: req.Identity.EffectID, AttemptID: req.AttemptID, PollRequestID: req.PollRequestID,
					ExternalJobID: req.ExternalJobID, ExternalRevision: revision,
					Disposition: DispositionAbsent,
				},
			}
			continue
		}
		result[req.PollRequestID] = EffectObservationResult{
			Observation: &EffectObservation{
				EffectID: req.Identity.EffectID, AttemptID: req.AttemptID, PollRequestID: req.PollRequestID,
				ExternalJobID: req.ExternalJobID, ExternalRevision: revision,
				Disposition: disposition, Retryable: retryable, Code: code, Reason: reason,
			},
		}
	}
	return result, nil
}

func (p *FakeDownloadProvider) EnsureReference(_ context.Context, req EnsureReferenceRequest) (EnsureReferenceResult, error) {
	p.service.AddReference(req.ExternalJobID, string(req.Identity.ReferenceID))
	return EnsureReferenceResult{
		EffectID: req.Identity.EffectID, ReferenceID: req.Identity.ReferenceID,
		RequestID: req.RequestID, ExternalJobID: req.ExternalJobID,
		ExternalRevision: 1, Disposition: EnsureBound,
		Failure: EnsureFailureNone,
	}, nil
}

func (p *FakeDownloadProvider) ReleaseEffect(_ context.Context, req ReleaseEffectRequest) (ReleaseEffectResult, error) {
	p.releaseCount++
	disp, failure := p.service.RemoveReference(string(req.Identity.ReferenceID), req.ExternalJobID)
	failureKind := ReleaseFailureNone
	if failure != nil {
		failureKind = *failure
	}
	return ReleaseEffectResult{
		EffectID: req.Identity.EffectID, ReferenceID: req.Identity.ReferenceID,
		ReleaseRequestID: req.ReleaseRequestID, ExternalJobID: req.ExternalJobID,
		ExternalRevision: 1, Disposition: disp, Failure: failureKind,
	}, nil
}

func jobDisposition(state FakeDownloadJobState) (EffectDisposition, bool, string, string) {
	switch state {
	case FakeJobQueued, FakeJobDownloading, FakeJobPaused, FakeJobVerifying:
		return DispositionStillActive, false, string(state), "in progress"
	case FakeJobReady:
		return DispositionCompleted, false, "", ""
	case FakeJobCancelling:
		return DispositionStillActive, false, string(state), "cancelling"
	case FakeJobCancelled:
		return DispositionCancelled, false, "", ""
	case FakeJobFailed:
		return DispositionFailed, true, "download_failed", "download failed"
	default:
		return DispositionStillActive, false, "unknown", "unknown state"
	}
}
