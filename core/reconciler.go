package core

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/akzj/converge/pkg/model"
)

// Reconciler is the core of Converge: it maintains managed configurations,
// computes diffs, builds DAGs, and drives execution through the safety layer.
type Reconciler struct {
	mu sync.Mutex

	providers   map[string]Provider  // provider name → implementation
	store       StateStore
	events      EventBus
	arbiter     Arbiter
	journal     Journal

	configs     map[string]*model.ManagedConfig  // config name → managed state
	globalGraph *model.Graph                     // cross-config DAG

	pendingDesired chan model.DesiredState  // inbound desired state updates
}

// NewReconciler creates a new Converge engine instance.
func NewReconciler(store StateStore, events EventBus, arbiter Arbiter, journal Journal) *Reconciler {
	return &Reconciler{
		providers:      make(map[string]Provider),
		store:          store,
		events:         events,
		arbiter:        arbiter,
		journal:        journal,
		configs:        make(map[string]*model.ManagedConfig),
		globalGraph:    &model.Graph{Nodes: make(map[string]*model.Node)},
		pendingDesired: make(chan model.DesiredState, 128),
	}
}

// RegisterProvider adds a provider to the engine.
func (r *Reconciler) RegisterProvider(p Provider) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.providers[p.Type()] = p
	log.Printf("converge: registered provider %q", p.Type())
}

// SubmitDesired queues a desired state for reconciliation.
// This is how the center (or user) tells Converge "this is what I want."
func (r *Reconciler) SubmitDesired(ctx context.Context, desired model.DesiredState) error {
	select {
	case r.pendingDesired <- desired:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Run starts the reconciliation loop. Blocks until ctx is cancelled.
func (r *Reconciler) Run(ctx context.Context) error {
	log.Println("converge: starting reconciliation loop")

	// Recover state from StateStore on startup
	if err := r.recover(ctx); err != nil {
		return fmt.Errorf("converge: recovery failed: %w", err)
	}

	// Subscribe to events from our own executor
	eventCh, err := r.events.Subscribe(ctx, "")
	if err != nil {
		return fmt.Errorf("converge: subscribe failed: %w", err)
	}

	ticker := time.NewTicker(30 * time.Second) // periodic drift detection
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("converge: reconciliation loop stopped")
			return ctx.Err()

		case desired := <-r.pendingDesired:
			r.handleDesired(ctx, desired)

		case event := <-eventCh:
			r.handleEvent(ctx, event)

		case <-ticker.C:
			r.tick(ctx)
		}

		// After handling any event, try to execute ready nodes
		r.executeReady(ctx)
	}
}

// recover loads previously recorded state into the engine on startup.
func (r *Reconciler) recover(ctx context.Context) error {
	ids, err := r.store.List(ctx)
	if err != nil {
		return err
	}
	for _, id := range ids {
		recorded, err := r.store.Get(ctx, id)
		if err != nil || recorded == nil {
			continue
		}
		r.configs[id.Name] = &model.ManagedConfig{
			ID:       id,
			Recorded: *recorded,
			Status:   model.ConfigConverged,
		}
		log.Printf("converge: recovered config %q (version %d, status %s)",
			id.Name, recorded.DesiredVersion, recorded.Status)
	}
	return nil
}

// handleDesired processes a new desired state from the center/user.
func (r *Reconciler) handleDesired(ctx context.Context, desired model.DesiredState) {
	r.mu.Lock()
	defer r.mu.Unlock()

	name := desired.ConfigID.Name
	existing, exists := r.configs[name]

	if !exists {
		log.Printf("converge: new config %q version %d", name, desired.Version)
		r.configs[name] = &model.ManagedConfig{
			ID:     desired.ConfigID,
			Desired: desired,
			Status: model.ConfigConverging,
			Graph:  &model.Graph{Nodes: make(map[string]*model.Node)},
		}
		r.reconcile(ctx, r.configs[name])
		return
	}

	// Skip if same version already applied
	if existing.Recorded.DesiredVersion == desired.Version &&
		existing.Recorded.DesiredDigest == desired.Digest {
		return
	}

	log.Printf("converge: config %q updated to version %d (was %d)",
		name, desired.Version, existing.Recorded.DesiredVersion)
	existing.Desired = desired
	existing.Status = model.ConfigConverging
	r.reconcile(ctx, existing)
}

// reconcile computes the diff and builds a DAG for one configuration.
func (r *Reconciler) reconcile(ctx context.Context, mc *model.ManagedConfig) {
	provider, ok := r.providers[mc.Desired.ProviderType]
	if !ok {
		log.Printf("converge: no provider %q for config %q", mc.Desired.ProviderType, mc.ID.Name)
		mc.Status = model.ConfigError
		return
	}

	// Inspect current state
	observed, err := provider.Inspect(ctx, model.ResourceID{Name: mc.ID.Name})
	if err != nil {
		log.Printf("converge: inspect failed for %q: %v", mc.ID.Name, err)
		mc.Status = model.ConfigError
		return
	}
	mc.Observed = observed

	// Compute diff
	ops, err := provider.Diff(ctx, observed, mc.Desired)
	if err != nil {
		log.Printf("converge: diff failed for %q: %v", mc.ID.Name, err)
		mc.Status = model.ConfigError
		return
	}

	// Build DAG
	graph := &model.Graph{Nodes: make(map[string]*model.Node)}
	for _, op := range ops {
		op.ConfigID = mc.ID.Name
		op.Provider = provider.Type()
		n := &model.Node{Operation: op, Status: model.NodePending}
		graph.Nodes[op.ID] = n
	}
	mc.Graph = graph

	// Merge into global graph (for cross-config dependencies)
	r.mergeGlobalGraph(ctx, mc)

	log.Printf("converge: config %q: diff produced %d operations", mc.ID.Name, len(ops))
}

// mergeGlobalGraph merges this config's DAG into the global cross-config DAG.
func (r *Reconciler) mergeGlobalGraph(ctx context.Context, mc *model.ManagedConfig) {
	for id, node := range mc.Graph.Nodes {
		r.globalGraph.Nodes[id] = node
	}
}

// handleEvent processes an Event from a completed Operation.
func (r *Reconciler) handleEvent(ctx context.Context, event model.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Update node status
	if node, ok := r.globalGraph.Nodes[event.NodeID]; ok {
		switch event.State {
		case model.StepCompleted:
			node.Status = model.NodeCompleted
		case model.StepFailed:
			node.Status = model.NodeFailed
		case model.StepCancelled:
			node.Status = model.NodeCancelled
		default:
			return
		}
	}

	// Journal the event
	if err := r.journal.Append(ctx, event); err != nil {
		log.Printf("converge: journal append failed: %v", err)
	}
}

// executeReady finds and executes all ready nodes from the global graph.
func (r *Reconciler) executeReady(ctx context.Context) {
	r.mu.Lock()
	nodes := r.globalGraph.ReadyNodes()
	r.mu.Unlock()

	for _, node := range nodes {
		go r.executeNode(ctx, node)
	}
}

// executeNode runs one Operation through the safety layer.
func (r *Reconciler) executeNode(ctx context.Context, node *model.Node) {
	r.mu.Lock()
	node.Status = model.NodeRunning
	op := node.Operation
	provider := r.providers[op.Provider]
	r.mu.Unlock()

	// Safety layer for destructive operations
	var release func()
	if op.Destructive && op.Phase == model.PhaseCommit {
		// Acquire local lease
		var err error
		release, err = r.arbiter.Acquire(ctx, op.ID)
		if err != nil {
			r.emitEvent(ctx, model.Event{
				NodeID: op.ID, ConfigID: op.ConfigID,
				State: model.StepFailed,
				Result: model.StepResult{State: model.StepFailed,
					Code: "arbiter_busy", Reason: err.Error()},
			})
			return
		}
		defer release()
	}

	// Execute via provider
	result, err := provider.Execute(ctx, op)
	if err != nil {
		r.emitEvent(ctx, model.Event{
			NodeID: op.ID, ConfigID: op.ConfigID,
			State: model.StepFailed,
			Result: model.StepResult{State: model.StepFailed,
				Code: "execute_error", Reason: err.Error()},
		})
		return
	}

	r.emitEvent(ctx, model.Event{
		NodeID: op.ID, ConfigID: op.ConfigID,
		State:  result.State,
		Result: result,
	})
}

func (r *Reconciler) emitEvent(ctx context.Context, event model.Event) {
	if err := r.events.Publish(ctx, event); err != nil {
		log.Printf("converge: event publish failed: %v", err)
	}
}

// tick runs periodic drift detection.
func (r *Reconciler) tick(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, mc := range r.configs {
		if mc.Status != model.ConfigConverged {
			continue // already converging or in error
		}
		// Check if we need to re-inspect for drift
		// (simplified: always re-inspect converged configs periodically)
		r.reconcile(ctx, mc)
	}
}