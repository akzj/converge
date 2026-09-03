package core

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/akzj/converge/pkg/model"
)

type blockingObserveProvider struct {
	*FakeDownloadProvider
	entered chan struct{}
	once    sync.Once
}

func (p *blockingObserveProvider) ObserveEffects(ctx context.Context, _ []ObserveEffectRequest) (map[PollRequestID]EffectObservationResult, error) {
	p.once.Do(func() { close(p.entered) })
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestEffectControlRPCIsAsyncBoundedAndTimedOut(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := NewMemoryExecutionStore()
	registry := NewPlanRegistry(store)
	plan, _, err := registry.Install(ctx, 0, testPlan(t, "digest", model.Operation{Key: "apply", ExecutionKind: model.ExecutionDirect}))
	if err != nil {
		t.Fatal(err)
	}
	identity := beginBoundEffect(t, registry, plan, "effect", "ref", "job")

	provider := &blockingObserveProvider{FakeDownloadProvider: NewFakeDownloadProvider("digest", NewFakeDownloadService()), entered: make(chan struct{})}
	r := NewReconciler(NewMemoryStateStore(), store, NewMemoryEventBus(), NewMemoryArbiter(), NewMemoryJournal())
	r.registry = registry
	r.providerVersions["test"] = map[string]Provider{"digest": provider}
	r.controlTimeout = 20 * time.Millisecond

	started := time.Now()
	r.processDueControls(ctx)
	if elapsed := time.Since(started); elapsed > 50*time.Millisecond {
		t.Fatalf("control dispatch blocked caller for %s", elapsed)
	}
	select {
	case <-provider.entered:
	case <-time.After(time.Second):
		t.Fatal("control worker did not call provider")
	}
	deadline := time.Now().Add(time.Second)
	for len(r.controlSem) != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(r.controlSem) != 0 {
		t.Fatal("timed-out control worker did not release capacity")
	}
	effect, _, ok := registry.LookupEffectAndReference(plan.ConfigID, identity.EffectIdentity.EffectID, identity.EffectIdentity.ReferenceID)
	if !ok || effect.State != ExternalEffectUnknown {
		t.Fatalf("timed-out observe did not conservatively mark effect unknown: %#v", effect)
	}
}
