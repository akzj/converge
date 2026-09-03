package core

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/akzj/converge/pkg/model"
)

func TestRecoveryRebuildsInProgressConfigFromExecutionDesired(t *testing.T) {
	ctx := context.Background()
	executionStore := NewMemoryExecutionStore()
	registry := NewPlanRegistry(executionStore)
	desired := model.DesiredState{ConfigID: model.ConfigID{Name: "first-convergence"}, ProviderType: "recovery", Version: 2, Spec: []byte(`{"critical":true}`), DependsOn: []string{"upstream"}}
	desired.Digest = model.DesiredSpecDigest(desired.Spec)
	candidate, err := BuildCandidate(desired.ConfigID, desired, "recovery", "digest-recovery", []model.Operation{{Key: "apply", ExecutionKind: model.ExecutionDirect}})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := registry.Install(ctx, 0, candidate); err != nil {
		t.Fatal(err)
	}

	r := NewReconciler(NewMemoryStateStore(), executionStore, NewMemoryEventBus(), NewMemoryArbiter(), NewMemoryJournal())
	// No provider is registered: recovery must still reconstruct the managed
	// config before a later provider registration resumes planning.
	if err := r.recover(ctx); err != nil {
		t.Fatal(err)
	}
	r.mu.RLock()
	managed := r.configs[desired.ConfigID.Name]
	r.mu.RUnlock()
	if managed == nil {
		t.Fatal("execution-only config was not recovered")
	}
	if managed.Desired.Version != 2 || managed.Desired.Digest != desired.Digest || string(managed.Desired.Spec) != `{"critical":true}` {
		t.Fatalf("desired was not fully recovered: %#v", managed.Desired)
	}
	if len(managed.DependsOnConfigs) != 1 || managed.DependsOnConfigs[0] != "upstream" {
		t.Fatalf("dependencies not recovered: %#v", managed.DependsOnConfigs)
	}
	if managed.Status != model.ConfigConverging {
		t.Fatalf("status=%s, want converging", managed.Status)
	}
}

func TestRecoveryRestoresAcceptedDesiredWithoutPlan(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryExecutionStore()
	first := NewReconciler(NewMemoryStateStore(), store, NewMemoryEventBus(), NewMemoryArbiter(), NewMemoryJournal())
	desired := model.DesiredState{
		ConfigID: model.ConfigID{Name: "accepted-only"}, ProviderType: "not-registered", Version: 7,
		Spec: []byte(`{"critical":true}`),
	}
	desired.Digest = model.DesiredSpecDigest(desired.Spec)
	if err := first.SubmitDesired(ctx, desired); err != nil {
		t.Fatal(err)
	}
	if snapshot, err := store.LoadExecution(ctx, desired.ConfigID); err != nil || snapshot == nil || snapshot.Plan != nil || snapshot.AcceptedDesired == nil {
		t.Fatalf("accepted desired was not persisted before planning: snapshot=%#v err=%v", snapshot, err)
	}

	recovered := NewReconciler(NewMemoryStateStore(), store, NewMemoryEventBus(), NewMemoryArbiter(), NewMemoryJournal())
	if err := recovered.recover(ctx); err != nil {
		t.Fatal(err)
	}
	config, ok := recovered.Config(desired.ConfigID.Name)
	if !ok || config.Desired.Version != desired.Version || config.Desired.Digest != desired.Digest {
		t.Fatalf("accepted desired not recovered: %#v", config)
	}
}

func TestSubmitDesiredValidatesDigestAndRevision(t *testing.T) {
	r := NewReconciler(NewMemoryStateStore(), NewMemoryExecutionStore(), NewMemoryEventBus(), NewMemoryArbiter(), NewMemoryJournal())
	desired := model.DesiredState{ConfigID: model.ConfigID{Name: "validated"}, ProviderType: "test", Version: 2, Spec: []byte(`{"v":2}`)}
	desired.Digest = model.DesiredSpecDigest(desired.Spec)
	if err := r.SubmitDesired(context.Background(), desired); err != nil {
		t.Fatal(err)
	}
	badDigest := desired
	badDigest.Version++
	badDigest.Digest = "sha256:not-the-spec"
	if err := r.SubmitDesired(context.Background(), badDigest); err == nil {
		t.Fatal("expected digest rejection")
	}
	older := desired
	older.Version--
	if err := r.SubmitDesired(context.Background(), older); !errors.Is(err, ErrDesiredConflict) {
		t.Fatalf("err=%v, want ErrDesiredConflict", err)
	}
}

func TestConfigReportsPlanningFailureAfterDurableAcceptance(t *testing.T) {
	r := NewReconciler(NewMemoryStateStore(), NewMemoryExecutionStore(), NewMemoryEventBus(), NewMemoryArbiter(), NewMemoryJournal())
	desired := model.DesiredState{ConfigID: model.ConfigID{Name: "missing-provider"}, ProviderType: "missing", Version: 1}
	desired.Digest = model.DesiredSpecDigest(desired.Spec)
	if err := r.SubmitDesired(context.Background(), desired); err != nil {
		t.Fatal(err)
	}
	r.planLatest(context.Background(), desired.ConfigID.Name)
	config, ok := r.Config(desired.ConfigID.Name)
	if !ok || config.Status != model.ConfigError || !strings.Contains(config.LastError, "not registered") {
		t.Fatalf("planning status does not expose failure: %#v", config)
	}
}

func TestPlanCloneDeepCopiesDesired(t *testing.T) {
	desired := model.DesiredState{Spec: []byte(`{"x":1}`), DependsOn: []string{"a"}}
	plan := &model.Plan{Desired: desired, Nodes: map[model.OperationKey]*model.Node{}}
	clone := plan.Clone()
	clone.Desired.Spec[0] = '['
	clone.Desired.DependsOn[0] = "changed"
	if string(plan.Desired.Spec) != `{"x":1}` || plan.Desired.DependsOn[0] != "a" {
		t.Fatalf("desired clone aliased original: %#v", plan.Desired)
	}
}
