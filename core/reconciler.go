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

	providers map[string]Provider // provider name → implementation
	store     StateStore
	events    EventBus
	arbiter   Arbiter
	journal   Journal

	configs     map[string]*model.ManagedConfig // config name → managed state
	globalGraph *model.Graph                    // cross-config DAG

	// inFlight tracks running operations and their cancel functions.
	inFlight map[string]context.CancelFunc // operation ID → cancel

	pendingDesired chan model.DesiredState // inbound desired state updates
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
		inFlight:       make(map[string]context.CancelFunc),
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

	if err := r.recover(ctx); err != nil {
		return fmt.Errorf("converge: recovery failed: %w", err)
	}

	eventCh, err := r.events.Subscribe(ctx, "")
	if err != nil {
		return fmt.Errorf("converge: subscribe failed: %w", err)
	}

	ticker := time.NewTicker(30 * time.Second)
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

// ---------------------------------------------------------------------------
// handleDesired — process a new desired state with supersession
// ---------------------------------------------------------------------------

// handleDesired processes a new desired state, handling plan supersession
// if the configuration is currently converging with in-flight operations.
func (r *Reconciler) handleDesired(ctx context.Context, desired model.DesiredState) {
	r.mu.Lock()

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
		r.mu.Unlock()
		r.reconcile(ctx, r.configs[name])
		return
	}

	// Skip if same version already applied
	if existing.Recorded.DesiredVersion == desired.Version &&
		existing.Recorded.DesiredDigest == desired.Digest {
		r.mu.Unlock()
		return
	}

	log.Printf("converge: config %q supersession version %d (was %d)",
		name, desired.Version, existing.Recorded.DesiredVersion)

	// Save the desired and release lock before supersession
	existing.Desired = desired
	existing.Status = model.ConfigConverging
	r.mu.Unlock()

	// Phase 1: Supersession — cancel or wait for in-flight operations
	r.supersede(ctx, name)

	// Phase 2: reconcile with fresh Inspect
	r.reconcile(ctx, existing)
}

// supersede handles in-flight operations when a new desired state arrives.
// Must NOT be called with r.mu held.
//
// Classification:
//   - CancelMode Safe: cancel immediately via context
//   - CancelMode Async: cancel (provider handles between sub-steps)
//   - CancelMode None: let complete (non-cancellable; re-Inspect will account for result)
func (r *Reconciler) supersede(ctx context.Context, configName string) {
	r.mu.Lock()
	var runningIDs []string
	for id, cancel := range r.inFlight {
		node, ok := r.globalGraph.Nodes[id]
		if !ok || node.Operation.ConfigID != configName {
			continue
		}
		if node.Status == model.NodeRunning {
			runningIDs = append(runningIDs, id)
			_ = cancel
		}
	}
	if len(runningIDs) == 0 {
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()

	log.Printf("converge: superseding %d in-flight ops for config %q", len(runningIDs), configName)

	// Cancel all cancellable operations; wait for non-cancellable ones
	var waitFor []string
	for _, id := range runningIDs {
		r.mu.Lock()
		node, ok := r.globalGraph.Nodes[id]
		cancelFn, inFlight := r.inFlight[id]
		r.mu.Unlock()

		if !ok || !inFlight {
			continue
		}

		switch node.Operation.CancelMode {
		case model.CancelModeNone:
			waitFor = append(waitFor, id)
			log.Printf("converge:   op %q: non-cancellable, waiting", id)
		default:
			cancelFn()
			r.mu.Lock()
			node.Status = model.NodeCancelled
			delete(r.inFlight, id)
			r.mu.Unlock()
			log.Printf("converge:   op %q: cancelled", id)
		}
	}

	// Wait for non-cancellable operations with timeout
	if len(waitFor) > 0 {
		r.waitForOps(ctx, waitFor, 30*time.Second)
	}
}

// waitForOps waits for a set of operations to complete, with timeout.
func (r *Reconciler) waitForOps(ctx context.Context, opIDs []string, timeout time.Duration) {
	remaining := make(map[string]struct{}, len(opIDs))
	for _, id := range opIDs {
		remaining[id] = struct{}{}
	}

	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for len(remaining) > 0 {
		select {
		case <-waitCtx.Done():
			log.Printf("converge: timeout waiting for %d ops to complete", len(remaining))
			return
		default:
		}

		r.mu.Lock()
		for id := range remaining {
			if node, ok := r.globalGraph.Nodes[id]; ok {
				if node.Status == model.NodeCompleted || node.Status == model.NodeFailed {
					delete(remaining, id)
					delete(r.inFlight, id)
				}
			}
		}
		r.mu.Unlock()

		if len(remaining) > 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// ---------------------------------------------------------------------------
// reconcile — Inspect → Diff → DAG
// ---------------------------------------------------------------------------

// reconcile computes the diff and builds a DAG for one configuration.
func (r *Reconciler) reconcile(ctx context.Context, mc *model.ManagedConfig) {
	r.mu.Lock()
	provider, ok := r.providers[mc.Desired.ProviderType]
	r.mu.Unlock()

	if !ok {
		log.Printf("converge: no provider %q for config %q", mc.Desired.ProviderType, mc.ID.Name)
		r.mu.Lock()
		mc.Status = model.ConfigError
		r.mu.Unlock()
		return
	}

	// Inspect current state
	observed, err := provider.Inspect(ctx, model.ResourceID{Name: mc.ID.Name})
	if err != nil {
		log.Printf("converge: inspect failed for %q: %v", mc.ID.Name, err)
		r.mu.Lock()
		mc.Status = model.ConfigError
		r.mu.Unlock()
		return
	}

	r.mu.Lock()
	mc.Observed = observed
	r.mu.Unlock()

	// Compute diff
	ops, err := provider.Diff(ctx, observed, mc.Desired)
	if err != nil {
		log.Printf("converge: diff failed for %q: %v", mc.ID.Name, err)
		r.mu.Lock()
		mc.Status = model.ConfigError
		r.mu.Unlock()
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

	r.mu.Lock()
	mc.Graph = graph
	r.mergeGlobalGraphLocked(mc)
	r.mu.Unlock()

	log.Printf("converge: config %q: diff produced %d operations", mc.ID.Name, len(ops))
}

// mergeGlobalGraphLocked merges this config's DAG into the global graph.
// Caller must hold r.mu.
func (r *Reconciler) mergeGlobalGraphLocked(mc *model.ManagedConfig) {
	// Remove old nodes for this config
	for id, node := range r.globalGraph.Nodes {
		if node.Operation.ConfigID == mc.ID.Name {
			delete(r.globalGraph.Nodes, id)
		}
	}
	// Add new nodes
	for id, node := range mc.Graph.Nodes {
		r.globalGraph.Nodes[id] = node
	}
}

// ---------------------------------------------------------------------------
// Event handling
// ---------------------------------------------------------------------------

// handleEvent processes an Event from a completed Operation.
func (r *Reconciler) handleEvent(ctx context.Context, event model.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()

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

	// Clean up in-flight tracking
	delete(r.inFlight, event.NodeID)

	// Record in journal
	if err := r.journal.Append(ctx, event); err != nil {
		log.Printf("converge: journal append failed: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

// executeReady finds and executes all ready nodes from the global graph.
func (r *Reconciler) executeReady(ctx context.Context) {
	r.mu.Lock()
	nodes := r.globalGraph.ReadyNodes()
	r.mu.Unlock()

	for _, node := range nodes {
		n := node
		go r.executeNode(ctx, n)
	}
}

// executeNode runs one Operation through the safety layer.
func (r *Reconciler) executeNode(ctx context.Context, node *model.Node) {
	op := node.Operation

	// Create cancellable context for this operation
	opCtx, opCancel := context.WithCancel(ctx)
	defer opCancel()

	r.mu.Lock()
	node.Status = model.NodeRunning
	r.inFlight[op.ID] = opCancel
	provider := r.providers[op.Provider]
	r.mu.Unlock()

	// Safety layer for destructive operations
	var release func()
	if op.Destructive && op.Phase == model.PhaseCommit {
		var err error
		release, err = r.arbiter.Acquire(opCtx, op.ID)
		if err != nil {
			r.emitEvent(opCtx, model.Event{
				NodeID: op.ID, ConfigID: op.ConfigID,
				State: model.StepFailed,
				Result: model.StepResult{State: model.StepFailed,
					Code: "arbiter_busy", Reason: err.Error()},
			})
			r.mu.Lock()
			delete(r.inFlight, op.ID)
			r.mu.Unlock()
			return
		}
		defer release()
	}

	// Execute via provider (uses opCtx which can be cancelled)
	result, err := provider.Execute(opCtx, op)
	if err != nil {
		r.emitEvent(opCtx, model.Event{
			NodeID: op.ID, ConfigID: op.ConfigID,
			State: model.StepFailed,
			Result: model.StepResult{State: model.StepFailed,
				Code: "execute_error", Reason: err.Error()},
		})
		r.mu.Lock()
		delete(r.inFlight, op.ID)
		r.mu.Unlock()
		return
	}

	r.emitEvent(opCtx, model.Event{
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

// ---------------------------------------------------------------------------
// Drift detection
// ---------------------------------------------------------------------------

// tick runs periodic drift detection.
func (r *Reconciler) tick(ctx context.Context) {
	r.mu.Lock()
	configs := make([]*model.ManagedConfig, 0, len(r.configs))
	for _, mc := range r.configs {
		if mc.Status == model.ConfigConverged {
			configs = append(configs, mc)
		}
	}
	r.mu.Unlock()

	for _, mc := range configs {
		r.reconcile(ctx, mc)
	}
}
