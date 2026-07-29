package core

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// FakeDownloadJobState models one external download job.
type FakeDownloadJobState string

const (
	FakeJobQueued      FakeDownloadJobState = "queued"
	FakeJobDownloading FakeDownloadJobState = "downloading"
	FakeJobPaused      FakeDownloadJobState = "paused"
	FakeJobVerifying   FakeDownloadJobState = "verifying"
	FakeJobReady       FakeDownloadJobState = "ready"
	FakeJobCancelling  FakeDownloadJobState = "cancelling"
	FakeJobCancelled   FakeDownloadJobState = "cancelled"
	FakeJobFailed      FakeDownloadJobState = "failed"
)

type fakeJob struct {
	ID             string
	IdempotencyKey string
	ArtifactID     string
	State          FakeDownloadJobState
	Revision       uint64
	References     map[string]bool // ReferenceID → active
	CreatedAt      time.Time
}

// FakeDownloadService is an in-memory correctness simulator for the external
// download service. It supports idempotent job creation, monotonic revisions,
// reference tracking, last-reference cancellation, and injectable errors.
type FakeDownloadService struct {
	mu    sync.RWMutex
	jobs  map[string]*fakeJob
	byKey map[string]string // idempotency key → job ID
	seq   atomic.Uint64

	// Injectables for Phase E fault matrix.
	DropNextEnsureResponse bool // create job then pretend RPC lost
	NextEnsureError        error
	NextObserveError       error
	GoneJobs               map[string]bool
}

func NewFakeDownloadService() *FakeDownloadService {
	return &FakeDownloadService{
		jobs:     make(map[string]*fakeJob),
		byKey:    make(map[string]string),
		GoneJobs: make(map[string]bool),
	}
}

// CreateOrGetJobAndEnsureReference is the atomic external operation.
// It returns the existing job if the idempotency key is known, otherwise creates
// one. The reference is always added to the set.
func (s *FakeDownloadService) CreateOrGetJobAndEnsureReference(idempotencyKey, artifactID, referenceID string) (jobID string, revision uint64, newJob bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.NextEnsureError != nil {
		err = s.NextEnsureError
		s.NextEnsureError = nil
		return "", 0, false, err
	}
	existingID, exists := s.byKey[idempotencyKey]
	if exists {
		job := s.jobs[existingID]
		if job != nil {
			if job.References == nil {
				job.References = make(map[string]bool)
			}
			job.References[referenceID] = true
			if s.DropNextEnsureResponse {
				s.DropNextEnsureResponse = false
				return "", 0, false, fmt.Errorf("ensure response lost after create")
			}
			return job.ID, job.Revision, false, nil
		}
	}
	jobID = fmt.Sprintf("download-%d", s.seq.Add(1))
	job := &fakeJob{
		ID:             jobID,
		IdempotencyKey: idempotencyKey,
		ArtifactID:     artifactID,
		State:          FakeJobQueued,
		Revision:       1,
		References:     map[string]bool{referenceID: true},
		CreatedAt:      time.Now(),
	}
	s.jobs[jobID] = job
	s.byKey[idempotencyKey] = jobID
	if s.DropNextEnsureResponse {
		s.DropNextEnsureResponse = false
		return "", 0, true, fmt.Errorf("ensure response lost after create")
	}
	return jobID, 1, true, nil
}

// AdvanceJob moves the job to the next expected state for testing.
func (s *FakeDownloadService) AdvanceJob(jobID string, target FakeDownloadJobState) error {
	return s.advanceJobAt(jobID, target, false)
}

func (s *FakeDownloadService) advanceJobAt(jobID string, target FakeDownloadJobState, force bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	job := s.jobs[jobID]
	if job == nil {
		return fmt.Errorf("job %q not found", jobID)
	}
	if !force && !s.validTransition(job.State, target) {
		return fmt.Errorf("invalid job transition %s -> %s", job.State, target)
	}
	job.State = target
	job.Revision++
	return nil
}

func (s *FakeDownloadService) validTransition(from, to FakeDownloadJobState) bool {
	switch from {
	case FakeJobQueued:
		return to == FakeJobDownloading || to == FakeJobFailed
	case FakeJobDownloading:
		return to == FakeJobPaused || to == FakeJobVerifying || to == FakeJobFailed
	case FakeJobPaused:
		return to == FakeJobDownloading || to == FakeJobFailed
	case FakeJobVerifying:
		return to == FakeJobReady || to == FakeJobFailed || to == FakeJobDownloading
	case FakeJobReady:
		return false // terminal
	case FakeJobCancelling:
		return to == FakeJobCancelled || to == FakeJobReady
	case FakeJobCancelled, FakeJobFailed:
		return false
	default:
		return false
	}
}

// AddReference adds a reference to an existing job. Used by EnsureReference.
func (s *FakeDownloadService) AddReference(jobID, referenceID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	job := s.jobs[jobID]
	if job == nil {
		return
	}
	if job.References == nil {
		job.References = make(map[string]bool)
	}
	job.References[referenceID] = true
	job.Revision++
}

// GetJob returns the current job snapshot.
func (s *FakeDownloadService) GetJob(jobID string) (state FakeDownloadJobState, revision uint64, references []string, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.GoneJobs[jobID] {
		return "", 0, nil, fmt.Errorf("job %q gone", jobID)
	}
	job := s.jobs[jobID]
	if job == nil {
		return "", 0, nil, fmt.Errorf("job %q not found", jobID)
	}
	refs := make([]string, 0, len(job.References))
	for ref := range job.References {
		refs = append(refs, ref)
	}
	return job.State, job.Revision, refs, nil
}

// MarkGone simulates authoritative Gone for a job.
func (s *FakeDownloadService) MarkGone(jobID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.GoneJobs[jobID] = true
	delete(s.jobs, jobID)
}

func (s *FakeDownloadService) isGone(jobID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.GoneJobs[jobID]
}

func (s *FakeDownloadService) consumeObserveError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.NextObserveError
	s.NextObserveError = nil
	return err
}

// RemoveReference idempotently removes one reference. If no references remain,
// it returns a last-reference-cancellation disposition and moves the job to
// cancelling. If the job is already terminal, released is returned.
func (s *FakeDownloadService) RemoveReference(referenceID, jobID string) (ReleaseDisposition, *ReleaseFailureKind) {
	s.mu.Lock()
	defer s.mu.Unlock()
	job := s.jobs[jobID]
	if job == nil {
		return ReleaseConfirmed, nil
	}
	if _, exists := job.References[referenceID]; !exists {
		return ReleaseConfirmed, nil
	}
	delete(job.References, referenceID)
	if len(job.References) > 0 {
		job.Revision++
		return ReleaseStillReferenced, nil
	}
	// Last reference: request cancellation.
	if job.State == FakeJobQueued || job.State == FakeJobDownloading || job.State == FakeJobPaused || job.State == FakeJobVerifying {
		job.State = FakeJobCancelling
	}
	job.Revision++
	kind := ReleaseLastReferenceCancelRequested
	return kind, nil
}

// IsCancellingOrCancelled returns true if the job is in a terminal or
// cancelling state.
func (s *FakeDownloadService) IsCancellingOrCancelled(jobID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job := s.jobs[jobID]
	if job == nil {
		return true
	}
	return job.State == FakeJobCancelling || job.State == FakeJobCancelled || job.State == FakeJobReady || job.State == FakeJobFailed
}
