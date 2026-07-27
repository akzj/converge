package core

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

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
	pendingDelete  chan string             // inbound config deletion requests
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
		pendingDelete:  make(chan string, 128),
	}
}

// RegisterProvider adds a provider to the engine. If a provider with the same
// Type() was already registered with a different Digest(), all configs using
// that provider are forced to re-reconcile (provider upgrade detection).
// RegisterProvider adds a provider to the engine. If a provider with the same
// Type() was already registered with a different Digest(), all configs using
// that provider are forced to re-reconcile (provider upgrade detection).
// NOTE: For cold-start (first registration), digest comparison with recorded
// state happens in recover() or on next reconcile() cycle.
func (r *Reconciler) RegisterProvider(p Provider) {
	r.mu.Lock()

	old, exists := r.providers[p.Type()]
	oldDigest := ""
	if exists {
		oldDigest = old.Digest()
	}
	r.providers[p.Type()] = p
	r.mu.Unlock()

	if !exists {
		zap.L().Info("converge: registered provider",
			zap.String("provider", p.Type()),
			zap.String("digest", p.Digest()))
		return
	}

	if oldDigest == p.Digest() {
		zap.L().Info("converge: re-registered provider (same digest)",
			zap.String("provider", p.Type()),
			zap.String("digest", p.Digest()))
		return
	}

	// Digest changed — collect configs using this provider and re-converge.
	// We process outside the lock because supersede/reconcile release it.
	r.mu.Lock()
	var targets []string
	for _, mc := range r.configs {
		if mc.Desired.ProviderType == p.Type() && mc.Recorded.HandlerDigest != p.Digest() {
			targets = append(targets, mc.ID.Name)
		}
	}
	r.mu.Unlock()

	zap.L().Info("converge: provider upgraded, re-reconciling affected configs",
		zap.String("provider", p.Type()),
		zap.String("old_digest", oldDigest),
		zap.String("new_digest", p.Digest()),
		zap.Int("targets", len(targets)))

	for _, name := range targets {
		r.supersede(context.Background(), name)
		r.mu.Lock()
		if mc, ok := r.configs[name]; ok {
			mc.Status = model.ConfigConverging
			if mc.Graph != nil {
				for id := range mc.Graph.Nodes {
					delete(r.globalGraph.Nodes, id)
				}
				mc.Graph.Nodes = make(map[string]*model.Node)
			}
		}
		r.mu.Unlock()
		r.reconcile(context.Background(), r.configs[name])
	}
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

// SubmitDelete queues a config for deletion. Dependents are deleted first.
func (r *Reconciler) SubmitDelete(ctx context.Context, configName string) error {
	select {
	case r.pendingDelete <- configName:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Run starts the reconciliation loop. Blocks until ctx is cancelled.
func (r *Reconciler) Run(ctx context.Context) error {
	zap.L().Info("converge: starting reconciliation loop")

	if err := r.recover(ctx); err != nil {
		return errors.Errorf("converge: recovery failed: %w", err)
	}

	eventCh, err := r.events.Subscribe(ctx, "")
	if err != nil {
		return errors.Errorf("converge: subscribe failed: %w", err)
	}

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			zap.L().Info("converge: reconciliation loop stopped")
			return ctx.Err()

		case desired := <-r.pendingDesired:
			r.handleDesired(ctx, desired)

		case name := <-r.pendingDelete:
			r.deleteConfig(ctx, name)

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

		status := model.ConfigConverged
		// Check if the recorded handler digest differs from the current provider
		if p, ok := r.providers[recorded.HandlerDigest]; !ok {
			// No provider registered yet — assume converged, will re-check on registration
		} else if p.Digest() != recorded.HandlerDigest {
			// Provider digest changed — force re-reconciliation
			status = model.ConfigConverging
			zap.L().Info("converge: recovered config with stale handler digest, will re-reconcile",
				zap.String("config", id.Name),
				zap.String("recorded", recorded.HandlerDigest),
				zap.String("current", p.Digest()))
		}

		r.configs[id.Name] = &model.ManagedConfig{
			ID:       id,
			Recorded: *recorded,
			Status:   status,
		}
		zap.L().Info("converge: recovered config",
			zap.String("config", id.Name),
			zap.Uint64("version", recorded.DesiredVersion),
			zap.String("status", string(status)))
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
		zap.L().Info("converge: new config",
			zap.String("config", name),
			zap.Uint64("version", desired.Version))
		r.configs[name] = &model.ManagedConfig{
			ID:     desired.ConfigID,
			Desired: desired,
			Status: model.ConfigConverging,
			Graph:  &model.Graph{Nodes: make(map[string]*model.Node)},
			DependsOnConfigs: append([]string(nil), desired.DependsOn...),
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

	zap.L().Info("converge: config supersession",
		zap.String("config", name),
		zap.Uint64("version", desired.Version),
		zap.Uint64("previous_version", existing.Recorded.DesiredVersion))

	// Save the desired, sync DependsOnConfigs, and release lock before supersession
	existing.Desired = desired
	existing.DependsOnConfigs = append([]string(nil), desired.DependsOn...)
	existing.Status = model.ConfigConverging
	r.mu.Unlock()

	// Phase 1: Supersession — cancel or wait for in-flight operations
	r.supersede(ctx, name)

	// Phase 2: Invalidate downstream configs whose dependency has changed
	r.invalidateDependents(ctx, name)

	// Phase 3: reconcile with fresh Inspect
	r.reconcile(ctx, existing)
}
// invalidateDependents marks all configs that depend on configName as stale,
// forcing them to re-reconcile on the next loop.
// deleteConfig removes a config and all its dependents in reverse dependency order.
func (r *Reconciler) deleteConfig(ctx context.Context, name string) {
	r.mu.Lock()
	mc, exists := r.configs[name]
	if !exists {
		r.mu.Unlock()
		return
	}

	// Collect all configs that must be deleted before this one (dependents)
	dependents := r.collectDependentsLocked(name)
	r.mu.Unlock()

	// Delete dependents first (depth-first, reverse dependency order)
	for _, depName := range dependents {
		r.deleteConfig(ctx, depName)
	}

	// Now delete the target config itself
	r.mu.Lock()
	zap.L().Info("converge: deleting config",
		zap.String("config", name),
		zap.Uint64("version", mc.Desired.Version))

	// Cancel any in-flight operations
	r.mu.Unlock()
	r.supersede(ctx, name)
	r.mu.Lock()

	// Remove from StateStore
	if err := r.store.Delete(ctx, mc.ID); err != nil {
		zap.L().Error("converge: state store delete failed", zap.Error(err))
	}

	// Remove from global graph
	for id, node := range r.globalGraph.Nodes {
		if node.Operation.ConfigID == name {
			delete(r.globalGraph.Nodes, id)
		}
	}

	// Remove from configs map
	delete(r.configs, name)
	r.mu.Unlock()

	zap.L().Info("converge: config deleted", zap.String("config", name))
}

// collectDependentsLocked returns names of configs that directly or indirectly
// depend on configName. Must be called with r.mu held.
func (r *Reconciler) collectDependentsLocked(configName string) []string {
	var deps []string
	for _, mc := range r.configs {
		if mc.ID.Name == configName {
			continue
		}
		for _, dep := range mc.DependsOnConfigs {
			if dep == configName {
				deps = append(deps, mc.ID.Name)
				// Recursively collect transitive dependents
				transitive := r.collectDependentsLocked(mc.ID.Name)
				deps = append(deps, transitive...)
				break
			}
		}
	}
	// Return unique names preserving order
	seen := make(map[string]struct{}, len(deps))
	unique := make([]string, 0, len(deps))
	for _, d := range deps {
		if _, ok := seen[d]; !ok {
			seen[d] = struct{}{}
			unique = append(unique, d)
		}
	}
	return unique
}

// invalidateDependents marks all transitive downstream configs as stale,
// cancels their in-flight operations, and triggers re-reconciliation.
// Must NOT be called with r.mu held.
func (r *Reconciler) invalidateDependents(ctx context.Context, configName string) {
	r.mu.Lock()
	deps := r.collectDependentsLocked(configName)
	var targets []*model.ManagedConfig
	for _, name := range deps {
		if mc, ok := r.configs[name]; ok {
			targets = append(targets, mc)
		}
	}
	r.mu.Unlock()

	for _, mc := range targets {
		zap.L().Info("converge: cascading to downstream config",
			zap.String("downstream", mc.ID.Name),
			zap.String("upstream", configName))

		// Cancel in-flight operations
		r.supersede(ctx, mc.ID.Name)

		// Clear stale graph
		r.mu.Lock()
		mc.Status = model.ConfigConverging
		if mc.Graph != nil {
			for id := range mc.Graph.Nodes {
				delete(r.globalGraph.Nodes, id)
			}
			mc.Graph.Nodes = make(map[string]*model.Node)
		}
		r.mu.Unlock()

		// Re-reconcile
		r.reconcile(ctx, mc)
	}
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

	zap.L().Info("converge: superseding in-flight ops",
		zap.String("config", configName),
		zap.Int("count", len(runningIDs)))

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
			zap.L().Info("converge: waiting for non-cancellable op", zap.String("op", id))
		default:
			cancelFn()
			r.mu.Lock()
			node.Status = model.NodeCancelled
			delete(r.inFlight, id)
			r.mu.Unlock()
			zap.L().Info("converge: cancelled op", zap.String("op", id))
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
			zap.L().Warn("converge: timeout waiting for ops to complete",
				zap.Int("remaining", len(remaining)))
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

	// Record the current provider digest for this convergence cycle.
	// This ensures the recorded HandlerDigest is always up to date.
	mc.Recorded.HandlerDigest = provider.Digest()


	if !ok {
		zap.L().Error("converge: no provider for config",
			zap.String("provider", mc.Desired.ProviderType),
			zap.String("config", mc.ID.Name))
		r.mu.Lock()
		mc.Status = model.ConfigError
		r.mu.Unlock()
		return
	}
	// Provider digest detection — if the handler implementation changed,
	// force re-reconciliation even if the desired spec hasn't changed.
	providerDigest := provider.Digest()
	if mc.Recorded.HandlerDigest != "" && mc.Recorded.HandlerDigest != providerDigest {
		zap.L().Info("converge: handler digest changed, forcing re-reconciliation",
			zap.String("config", mc.ID.Name),
			zap.String("old", mc.Recorded.HandlerDigest),
			zap.String("new", providerDigest))
	}


	// If this config depends on others, check they are all converged first.
	if !r.dependenciesMet(mc) {
		zap.L().Info("converge: waiting for dependencies",
			zap.String("config", mc.ID.Name),
			zap.Strings("depends_on", mc.DependsOnConfigs))
		r.mu.Lock()
		mc.Status = model.ConfigConverging
		r.mu.Unlock()
		return
	}

	// Inspect current state
	observed, err := provider.Inspect(ctx, model.ResourceID{Name: mc.ID.Name})
	if err != nil {
		zap.L().Error("converge: inspect failed",
			zap.String("config", mc.ID.Name),
			zap.Error(err))
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
		zap.L().Error("converge: diff failed",
			zap.String("config", mc.ID.Name),
			zap.Error(err))
		r.mu.Lock()
		mc.Status = model.ConfigError
		r.mu.Unlock()
		return
	}

	// Build DAG
	if len(ops) == 0 {
		// Already converged — no operations needed
		r.mu.Lock()
		mc.Status = model.ConfigConverged
		mc.Graph = &model.Graph{Nodes: make(map[string]*model.Node)}
		recorded := model.RecordedState{
			ConfigID:        mc.Desired.ConfigID,
			DesiredVersion:  mc.Desired.Version,
			DesiredDigest:   mc.Desired.Digest,
			HandlerDigest:   provider.Digest(),
			Status:          string(model.ConfigConverged),
			UpdatedAt:       time.Now(),
		}
		if err := r.store.Record(ctx, recorded); err != nil {
		mc.Recorded = recorded // update in-memory state
			zap.L().Error("converge: state store record failed", zap.Error(err))
		}
		r.mu.Unlock()
		zap.L().Info("converge: config already converged",
			zap.String("config", mc.ID.Name),
			zap.Uint64("version", mc.Desired.Version))
		// Wake up downstream configs that depend on this one
		r.wakeUpDependents(ctx, mc.ID.Name)
		return
	}

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

	zap.L().Info("converge: diff produced operations",
		zap.String("config", mc.ID.Name),
		zap.Int("operations", len(ops)))
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
// dependenciesMet checks whether all configs this one depends on have converged.
func (r *Reconciler) dependenciesMet(mc *model.ManagedConfig) bool {
	if len(mc.DependsOnConfigs) == 0 {
		return true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, depName := range mc.DependsOnConfigs {
		dep, ok := r.configs[depName]
		if !ok || dep.Status != model.ConfigConverged {
			return false
		}
	}
	return true
}


// ---------------------------------------------------------------------------
// Event handling
// ---------------------------------------------------------------------------

// handleEvent processes an Event from a completed Operation.
func (r *Reconciler) handleEvent(ctx context.Context, event model.Event) {
	r.mu.Lock()

	if node, ok := r.globalGraph.Nodes[event.NodeID]; ok {
		switch event.State {
		case model.StepCompleted:
			node.Status = model.NodeCompleted
		case model.StepFailed:
			node.Status = model.NodeFailed
		case model.StepCancelled:
			node.Status = model.NodeCancelled
		default:
			r.mu.Unlock()
			return
		}
	}

	// Clean up in-flight tracking
	delete(r.inFlight, event.NodeID)

	// Record in journal
	if err := r.journal.Append(ctx, event); err != nil {
		zap.L().Error("converge: journal append failed", zap.Error(err))
	}

	// Check if this config's graph is fully converged
	if mc, ok := r.configs[event.ConfigID]; ok && mc.Graph != nil {
		if allNodesSucceeded(mc.Graph) {
			digest := mc.Recorded.HandlerDigest
			if digest == "" {
				// Fallback: try to get from provider (shouldn't happen after reconcile sets it)
				if p, ok := r.providers[mc.Desired.ProviderType]; ok {
					digest = p.Digest()
				}
			}
			recorded := model.RecordedState{
				ConfigID:        mc.Desired.ConfigID,
				DesiredVersion:  mc.Desired.Version,
				DesiredDigest:   mc.Desired.Digest,
				HandlerDigest:   digest,
				HandlerRef:      mc.Recorded.HandlerRef,
				Status:          string(model.ConfigConverged),
				UpdatedAt:       time.Now(),
			}
			mc.Recorded = recorded // update in-memory state
			if err := r.store.Record(ctx, recorded); err != nil {
				zap.L().Error("converge: state store record failed", zap.Error(err))
			}
			zap.L().Info("converge: config converged",
				zap.String("config", event.ConfigID),
				zap.Uint64("version", mc.Desired.Version))
			// Wake up downstream configs that depend on this one
			r.mu.Unlock()
			r.wakeUpDependents(ctx, event.ConfigID)
			return
		}

		// Check if any node failed — mark config error
		if hasFailedNode(mc.Graph) {
			mc.Status = model.ConfigError
			recorded := model.RecordedState{
				ConfigID:       mc.Desired.ConfigID,
				DesiredVersion: mc.Desired.Version,
				DesiredDigest:  mc.Desired.Digest,
				Status:         string(model.ConfigError),
				UpdatedAt:      time.Now(),
			}
			_ = r.store.Record(ctx, recorded)
			mc.Recorded = recorded
			zap.L().Error("converge: config failed",
				zap.String("config", event.ConfigID))
		}
	}

	r.mu.Unlock()
}

// hasFailedNode returns true if any node in the graph has failed.
func hasFailedNode(g *model.Graph) bool {
	for _, n := range g.Nodes {
		if n.Status == model.NodeFailed {
			return true
		}
	}
	return false
}

// wakeUpDependents triggers reconcile for all configs that depend on configName.
func (r *Reconciler) wakeUpDependents(ctx context.Context, configName string) {
	r.mu.Lock()
	var downstream []*model.ManagedConfig
	for _, mc := range r.configs {
		for _, dep := range mc.DependsOnConfigs {
			if dep == configName && mc.Status != model.ConfigConverged {
				downstream = append(downstream, mc)
				break
			}
		}
	}
	r.mu.Unlock()
	for _, mc := range downstream {
		zap.L().Info("converge: waking up downstream config",
			zap.String("config", mc.ID.Name),
			zap.String("depends_on", configName))
		r.reconcile(ctx, mc)
	}
}

// allNodesSucceeded returns true iff every node in the graph is either
// completed (success) or was never needed (empty graph).
// Failed/Cancelled nodes mean convergence failed.
func allNodesSucceeded(g *model.Graph) bool {
	for _, n := range g.Nodes {
		if n.Status != model.NodeCompleted {
			return false
		}
	}
	return true
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
		zap.L().Error("converge: event publish failed", zap.Error(err))
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
		// Scan both converged (drift detection) and converging (stale/blocked re-entry)
		if mc.Status == model.ConfigConverged || mc.Status == model.ConfigConverging {
			configs = append(configs, mc)
		}
	}
	r.mu.Unlock()

	for _, mc := range configs {
		r.reconcile(ctx, mc)
	}
}
