// Package converge provides the embeddable durable reconciliation runtime.
package converge

import (
	"context"
	"reflect"
	"slices"
	"sync"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/akzj/converge/core"
	"github.com/akzj/converge/pkg/model"
)

type SnapshotACK struct {
	Accepted bool   `json:"accepted"`
	Revision uint64 `json:"revision"`
	Digest   string `json:"digest"`
	Code     string `json:"code,omitempty"`
	Reason   string `json:"reason,omitempty"`
}

type SnapshotIdentity struct {
	Present  bool   `json:"present"`
	Revision uint64 `json:"revision,omitempty"`
	Digest   string `json:"digest,omitempty"`
}

type RuntimeStatus struct {
	Snapshot           SnapshotIdentity    `json:"snapshot"`
	DispatchedRevision uint64              `json:"dispatched_revision,omitempty"`
	ApplyError         string              `json:"apply_error,omitempty"`
	Configs            []core.ConfigReport `json:"configs"`
}

// Runtime owns durable snapshot acceptance and translates the latest complete
// snapshot into idempotent per-config reconciliation intents.
type Runtime struct {
	reconciler *core.Reconciler
	store      core.DesiredSnapshotStore
	sqlite     *core.SQLiteStore
	wake       chan struct{}

	mu                 sync.RWMutex
	dispatchedRevision uint64
	applyError         string
	ready              bool
	started            bool
	running            bool
	closed             bool
}

var (
	ErrRuntimeAlreadyRunning = errors.New("converge runtime Run may only be called once")
	ErrRuntimeRunning        = errors.New("converge runtime is running")
	ErrRuntimeClosed         = errors.New("converge runtime is closed")
)

func newRuntime(reconciler *core.Reconciler, store core.DesiredSnapshotStore) (*Runtime, error) {
	if reconciler == nil {
		return nil, errors.New("converge runtime reconciler is nil")
	}
	if isNilDependency(store) {
		return nil, errors.New("converge runtime snapshot store is nil")
	}
	if err := reconciler.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid converge runtime reconciler")
	}
	return &Runtime{reconciler: reconciler, store: store, wake: make(chan struct{}, 1)}, nil
}

// OpenSQLiteRuntime opens a self-contained runtime backed by one SQLite
// database. Runtime owns the database and closes it from Close.
func OpenSQLiteRuntime(ctx context.Context, path string, options ...core.ReconcilerOption) (*Runtime, error) {
	store, err := core.OpenSQLite(ctx, path)
	if err != nil {
		return nil, err
	}
	reconciler, err := core.NewReconcilerChecked(store, store, core.NewMemoryEventBus(), core.NewMemoryArbiter(), store, options...)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	runtime, err := newRuntime(reconciler, store)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	runtime.sqlite = store
	return runtime, nil
}

// RegisterProvider registers a resource implementation. Register Providers
// before Run; a later digest for the same type is a rolling upgrade.
func (r *Runtime) RegisterProvider(ctx context.Context, provider core.Provider) error {
	if err := r.ensureOpen(); err != nil {
		return err
	}
	return r.reconciler.RegisterProviderChecked(ctx, provider)
}

// UnregisterProviderVersion removes an unused Provider version while stopped.
func (r *Runtime) UnregisterProviderVersion(providerType, providerDigest string) error {
	if err := r.ensureOpen(); err != nil {
		return err
	}
	return r.reconciler.UnregisterProviderVersion(providerType, providerDigest)
}

// SubmitSnapshot validates and persists the complete snapshot before returning
// an accepted ACK. Convergence happens asynchronously in Run.
func (r *Runtime) SubmitSnapshot(ctx context.Context, snapshot model.DesiredSnapshot) SnapshotACK {
	if err := r.ensureOpen(); err != nil {
		return SnapshotACK{Revision: snapshot.Revision, Digest: snapshot.Digest, Code: "runtime_closed", Reason: err.Error()}
	}
	if err := model.ValidateDesiredSnapshot(snapshot); err != nil {
		return SnapshotACK{Revision: snapshot.Revision, Digest: snapshot.Digest, Code: "invalid_snapshot", Reason: err.Error()}
	}
	accepted, err := r.store.AcceptDesiredSnapshot(ctx, snapshot)
	if err != nil {
		code := "persistence_failed"
		if errors.Is(err, core.ErrDesiredSnapshotConflict) {
			code = "revision_conflict"
		}
		return SnapshotACK{Revision: snapshot.Revision, Digest: snapshot.Digest, Code: code, Reason: err.Error()}
	}
	r.wakeApply()
	code := "accepted"
	if !accepted {
		code = "duplicate"
	}
	return SnapshotACK{Accepted: true, Revision: snapshot.Revision, Digest: snapshot.Digest, Code: code}
}

func (r *Runtime) CurrentSnapshot(ctx context.Context) (SnapshotIdentity, error) {
	if err := r.ensureOpen(); err != nil {
		return SnapshotIdentity{}, err
	}
	snapshot, err := r.store.LoadDesiredSnapshot(ctx)
	if err != nil {
		return SnapshotIdentity{}, err
	}
	if snapshot == nil {
		return SnapshotIdentity{}, nil
	}
	return SnapshotIdentity{Present: true, Revision: snapshot.Revision, Digest: snapshot.Digest}, nil
}

// Backup creates a transactionally consistent copy of the runtime database.
func (r *Runtime) Backup(ctx context.Context, destination string) error {
	if err := r.ensureOpen(); err != nil {
		return err
	}
	if r.sqlite == nil {
		return errors.New("converge runtime is not backed by SQLite")
	}
	return r.sqlite.Backup(ctx, destination)
}

func (r *Runtime) Status(ctx context.Context) (RuntimeStatus, error) {
	identity, err := r.CurrentSnapshot(ctx)
	if err != nil {
		return RuntimeStatus{}, err
	}
	r.mu.RLock()
	status := RuntimeStatus{Snapshot: identity, DispatchedRevision: r.dispatchedRevision, ApplyError: r.applyError}
	r.mu.RUnlock()
	status.Configs = r.reconciler.Reports()
	return status, nil
}

func (r *Runtime) ConfigStatus(name string) (core.ConfigReport, bool) {
	return r.reconciler.Report(name)
}

func (r *Runtime) Refresh(ctx context.Context, name string) error {
	if err := r.ensureOpen(); err != nil {
		return err
	}
	return r.reconciler.Refresh(ctx, name)
}

func (r *Runtime) Ready() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.ready
}

// Run recovers Core first, then replays the latest durable snapshot. A
// successful SubmitSnapshot does not depend on Run being online.
func (r *Runtime) Run(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrRuntimeClosed
	}
	if r.started {
		r.mu.Unlock()
		return ErrRuntimeAlreadyRunning
	}
	r.started = true
	r.running = true
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		r.running = false
		r.ready = false
		r.mu.Unlock()
	}()
	ready := make(chan error, 1)
	coreDone := make(chan error, 1)
	go func() { coreDone <- r.reconciler.RunWithReady(ctx, ready) }()
	select {
	case err := <-ready:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		<-coreDone
		return ctx.Err()
	}
	r.mu.Lock()
	r.ready = true
	r.mu.Unlock()
	if err := r.applyLatest(ctx); err != nil {
		r.recordApply(0, err)
	}
	retry := time.NewTicker(time.Second)
	defer retry.Stop()
	for {
		select {
		case <-ctx.Done():
			<-coreDone
			return ctx.Err()
		case err := <-coreDone:
			return err
		case <-r.wake:
			if err := r.applyLatest(ctx); err != nil {
				r.recordApply(0, err)
			}
		case <-retry.C:
			r.mu.RLock()
			retryNeeded := r.applyError != ""
			r.mu.RUnlock()
			if retryNeeded {
				if err := r.applyLatest(ctx); err != nil {
					r.recordApply(0, err)
				}
			}
		}
	}
}

// Close releases the SQLite database owned by a runtime created with
// OpenSQLiteRuntime. The caller must first cancel Run and wait for it to return.
// No other Runtime calls may race with Close. Close is idempotent.
func (r *Runtime) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.running {
		return ErrRuntimeRunning
	}
	if r.closed {
		return nil
	}
	if r.sqlite != nil {
		if err := r.sqlite.Close(); err != nil {
			return err
		}
	}
	r.closed = true
	return nil
}

func (r *Runtime) ensureOpen() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return ErrRuntimeClosed
	}
	return nil
}

func isNilDependency(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

func (r *Runtime) applyLatest(ctx context.Context) error {
	snapshot, err := r.store.LoadDesiredSnapshot(ctx)
	if err != nil || snapshot == nil {
		return err
	}
	items, err := dependencyOrder(snapshot.Items)
	if err != nil {
		return err
	}
	desiredNames := make(map[string]struct{}, len(items))
	for _, desired := range items {
		desiredNames[desired.ConfigID.Name] = struct{}{}
		if err := r.reconciler.SubmitDesired(ctx, desired); err != nil {
			return errors.Wrapf(err, "apply desired %q", desired.ConfigID.Name)
		}
	}
	for _, name := range r.reconciler.ConfigNames() {
		if _, retained := desiredNames[name]; retained {
			continue
		}
		current, exists := r.reconciler.Config(name)
		if !exists {
			continue
		}
		if err := r.reconciler.SubmitDeleteIfDesired(ctx, current.Desired); err != nil {
			return errors.Wrapf(err, "apply deletion %q", name)
		}
	}
	r.recordApply(snapshot.Revision, nil)
	return nil
}

func (r *Runtime) recordApply(revision uint64, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err != nil {
		r.applyError = err.Error()
		return
	}
	r.dispatchedRevision = revision
	r.applyError = ""
}

func (r *Runtime) wakeApply() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func dependencyOrder(items []model.DesiredState) ([]model.DesiredState, error) {
	byName := make(map[string]model.DesiredState, len(items))
	for _, item := range items {
		byName[item.ConfigID.Name] = model.CloneDesiredState(item)
	}
	var result []model.DesiredState
	colors := make(map[string]uint8, len(items))
	var visit func(string) error
	visit = func(name string) error {
		switch colors[name] {
		case 1:
			return errors.Errorf("dependency cycle contains %q", name)
		case 2:
			return nil
		}
		colors[name] = 1
		item := byName[name]
		dependencies := slices.Clone(item.DependsOn)
		slices.Sort(dependencies)
		for _, dependency := range dependencies {
			if _, exists := byName[dependency]; !exists {
				return errors.Errorf("config %q depends on missing config %q", name, dependency)
			}
			if err := visit(dependency); err != nil {
				return err
			}
		}
		colors[name] = 2
		result = append(result, item)
		return nil
	}
	names := make([]string, 0, len(byName))
	for name := range byName {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		if err := visit(name); err != nil {
			return nil, err
		}
	}
	return result, nil
}
