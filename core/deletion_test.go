package core

import (
	"context"
	"testing"
	"time"

	"github.com/akzj/converge/pkg/model"
)

func TestMarkDeletingStopsSchedulingAndPersistsTombstone(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryExecutionStore()
	registry := NewPlanRegistry(store)
	installed, _, err := registry.Install(ctx, 0, testPlan(t, "digest", model.Operation{Key: "running", CancelMode: model.CancelModeSafe}, model.Operation{Key: "pending"}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.StartAttempt(ctx, installed.ConfigID, installed.Generation, "running", "attempt"); err != nil {
		t.Fatal(err)
	}
	attempts, err := registry.MarkDeleting(ctx, installed.ConfigID)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 1 || attempts[0].Status != model.AttemptCancelling {
		t.Fatalf("deletion attempts=%#v", attempts)
	}
	if !registry.IsDeleting(installed.ConfigID) {
		t.Fatal("deleting tombstone not visible")
	}
	if plan, ready := registry.ReadyOperations(installed.ConfigID); plan != nil || len(ready) != 0 {
		t.Fatalf("deleting config remained schedulable: %#v %#v", plan, ready)
	}

	recovered := NewPlanRegistry(store)
	if err := recovered.Restore(ctx); err != nil {
		t.Fatal(err)
	}
	if !recovered.IsDeleting(installed.ConfigID) {
		t.Fatal("deleting tombstone not durable")
	}
	if plan, ready := recovered.ReadyOperations(installed.ConfigID); plan != nil || len(ready) != 0 {
		t.Fatalf("recovered deleting config schedulable: %#v %#v", plan, ready)
	}
}

func TestConditionalDeleteCannotRemoveNewerDesired(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	r := NewReconciler(NewMemoryStateStore(), NewMemoryExecutionStore(), NewMemoryEventBus(), NewMemoryArbiter(), NewMemoryJournal())
	r.RegisterProvider(ctx, &mockProvider{typeName: "test"})
	v1 := model.DesiredState{ConfigID: model.ConfigID{Name: "config"}, ProviderType: "test", Version: 1, Spec: []byte(`{"v":1}`)}
	v1.Digest = model.DesiredSpecDigest(v1.Spec)
	if err := r.SubmitDesired(ctx, v1); err != nil {
		t.Fatal(err)
	}
	if err := r.SubmitDeleteIfDesired(ctx, v1); err != nil {
		t.Fatal(err)
	}
	v2 := v1
	v2.Version = 2
	v2.Spec = []byte(`{"v":2}`)
	v2.Digest = model.DesiredSpecDigest(v2.Spec)
	if err := r.SubmitDesired(ctx, v2); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	defer func() {
		cancel()
		<-done
	}()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(r.pendingDelete) == 0 {
			time.Sleep(50 * time.Millisecond)
			if managed, ok := r.Config("config"); ok && managed.Desired.Version == 2 {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("stale conditional deletion removed the newer desired")
}

func TestReconcilerDeletionCascadesAndFinalizes(t *testing.T) {
	ctx := context.Background()
	stateStore := NewMemoryStateStore()
	executionStore := NewMemoryExecutionStore()
	r := NewReconciler(stateStore, executionStore, NewMemoryEventBus(), NewMemoryArbiter(), NewMemoryJournal())
	configs := []struct {
		name    string
		depends []string
	}{{"upstream", nil}, {"downstream", []string{"upstream"}}}
	for _, config := range configs {
		desired := model.DesiredState{ConfigID: model.ConfigID{Name: config.name}, ProviderType: "test", Version: 1, Digest: config.name, DependsOn: config.depends}
		candidate, err := BuildCandidate(desired.ConfigID, desired, "test", "digest", nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := r.registry.Install(ctx, 0, candidate); err != nil {
			t.Fatal(err)
		}
		recorded := model.RecordedState{ConfigID: desired.ConfigID, ProviderType: "test", DesiredVersion: 1, DesiredDigest: config.name, Status: model.ConfigConverged}
		if err := stateStore.Record(ctx, recorded); err != nil {
			t.Fatal(err)
		}
		r.configs[config.name] = &model.ManagedConfig{ID: desired.ConfigID, Desired: desired, Recorded: recorded, DependsOnConfigs: config.depends, Status: model.ConfigConverged}
	}
	r.deleteConfig(ctx, "upstream")
	if len(r.configs) != 0 {
		t.Fatalf("configs remain after cascade: %#v", r.configs)
	}
	for _, name := range []string{"upstream", "downstream"} {
		id := model.ConfigID{Name: name}
		if snapshot := r.registry.Snapshot(id); snapshot.Plan != nil {
			t.Fatalf("execution remains for %s", name)
		}
		if recorded, err := stateStore.Get(ctx, id); err != nil || recorded != nil {
			t.Fatalf("record remains for %s: %#v err=%v", name, recorded, err)
		}
	}
}

func TestMaintenanceReleaseCompletionFinalizesDeletion(t *testing.T) {
	ctx := context.Background()
	stateStore := NewMemoryStateStore()
	executionStore := NewMemoryExecutionStore()
	r := NewReconciler(stateStore, executionStore, NewMemoryEventBus(), NewMemoryArbiter(), NewMemoryJournal())
	provider := &releaseConfirmProvider{mockProvider: &mockProvider{typeName: "test"}}
	r.RegisterProvider(ctx, provider)

	plan, _, err := r.registry.Install(ctx, 0, testPlan(t, provider.Digest(),
		model.Operation{Key: "ensure", ExecutionKind: model.ExecutionEffectEnsure, EffectKey: "download"},
	))
	if err != nil {
		t.Fatal(err)
	}
	beginBoundEffect(t, r.registry, plan, "effect-delete", "reference-delete", "job-delete")
	recorded := model.RecordedState{
		ConfigID: plan.ConfigID, ProviderType: provider.Type(), DesiredVersion: 1,
		DesiredDigest: "desired-delete", Status: model.ConfigConverged,
	}
	if err := stateStore.Record(ctx, recorded); err != nil {
		t.Fatal(err)
	}
	r.configs[plan.ConfigID.Name] = &model.ManagedConfig{
		ID: plan.ConfigID, Recorded: recorded, Status: model.ConfigConverged,
		Desired: model.DesiredState{ConfigID: plan.ConfigID, ProviderType: provider.Type(), Version: 1, Digest: "desired-delete"},
	}
	if _, err := r.registry.MarkDeleting(ctx, plan.ConfigID); err != nil {
		t.Fatal(err)
	}
	controls, err := r.registry.ListDueControls(ctx, time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	var release DueControlRef
	for _, control := range controls {
		if control.ControlRequestID == "release-reference-delete" {
			release = control
			break
		}
	}
	if release.ControlRequestID == "" {
		t.Fatalf("release control not scheduled: %#v", controls)
	}
	if err := r.processOneDueControl(ctx, release, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, exists := r.Config(plan.ConfigID.Name); exists {
		t.Fatal("config remains after terminal maintenance release")
	}
	if snapshot := r.registry.Snapshot(plan.ConfigID); snapshot.Plan != nil {
		t.Fatalf("execution remains after deletion: %#v", snapshot)
	}
	if stored, err := stateStore.Get(ctx, plan.ConfigID); err != nil || stored != nil {
		t.Fatalf("recorded state remains: %#v err=%v", stored, err)
	}
}

type releaseConfirmProvider struct{ *mockProvider }

func (*releaseConfirmProvider) Digest() string { return "digest" }
func (*releaseConfirmProvider) EnsureEffect(context.Context, EnsureEffectRequest) (EnsureEffectResult, error) {
	return EnsureEffectResult{}, nil
}
func (*releaseConfirmProvider) ObserveEffects(context.Context, []ObserveEffectRequest) (map[PollRequestID]EffectObservationResult, error) {
	return nil, nil
}
func (*releaseConfirmProvider) EnsureReference(context.Context, EnsureReferenceRequest) (EnsureReferenceResult, error) {
	return EnsureReferenceResult{}, nil
}
func (*releaseConfirmProvider) ReleaseEffect(_ context.Context, request ReleaseEffectRequest) (ReleaseEffectResult, error) {
	return ReleaseEffectResult{
		EffectID: request.Identity.EffectID, ReferenceID: request.Identity.ReferenceID,
		ReleaseRequestID: request.ReleaseRequestID, ExternalJobID: request.ExternalJobID,
		ExternalRevision: request.ExternalRevision + 1, Disposition: ReleaseConfirmed, Failure: ReleaseFailureNone,
	}, nil
}
