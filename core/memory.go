package core

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/akzj/converge/pkg/model"
)

// MemoryStateStore is an in-memory StateStore for testing and development.
type MemoryStateStore struct {
	mu     sync.RWMutex
	states map[model.ConfigID]*model.RecordedState
}

func NewMemoryStateStore() *MemoryStateStore {
	return &MemoryStateStore{states: make(map[model.ConfigID]*model.RecordedState)}
}

func (s *MemoryStateStore) Get(_ context.Context, id model.ConfigID) (*model.RecordedState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	state, ok := s.states[id]
	if !ok {
		return nil, nil
	}
	copy := *state
	return &copy, nil
}

func (s *MemoryStateStore) List(_ context.Context) ([]model.ConfigID, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := make([]model.ConfigID, 0, len(s.states))
	for id := range s.states {
		ids = append(ids, id)
	}
	return ids, nil
}

func (s *MemoryStateStore) Record(_ context.Context, state model.RecordedState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	copy := state
	copy.UpdatedAt = time.Now()
	s.states[state.ConfigID] = &copy
	return nil
}

func (s *MemoryStateStore) Delete(_ context.Context, id model.ConfigID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.states, id)
	return nil
}

// MemoryEventBus is an in-memory EventBus for testing and development.
type MemoryEventBus struct {
	mu       sync.RWMutex
	channels map[string][]chan model.Event
}

func NewMemoryEventBus() *MemoryEventBus {
	return &MemoryEventBus{channels: make(map[string][]chan model.Event)}
}

func (b *MemoryEventBus) Publish(ctx context.Context, event model.Event) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	// MemoryEventBus provides at-least-once delivery while Publish's context
	// remains live. It deliberately applies backpressure instead of silently
	// dropping correctness-critical terminal events.
	for _, ch := range b.channels[event.ConfigID] {
		select {
		case ch <- event:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	for _, ch := range b.channels[""] {
		select {
		case ch <- event:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (b *MemoryEventBus) Subscribe(_ context.Context, configID string) (<-chan model.Event, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	ch := make(chan model.Event, 64)
	b.channels[configID] = append(b.channels[configID], ch)
	return ch, nil
}

// MemoryArbiter is an in-memory Arbiter for testing and development.
type MemoryArbiter struct {
	mu         sync.Mutex
	activeOpID string
}

func NewMemoryArbiter() *MemoryArbiter {
	return &MemoryArbiter{}
}

func (a *MemoryArbiter) Acquire(_ context.Context, operationID string) (func(), error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.activeOpID != "" {
		return nil, errors.Errorf("destructive commit busy: %s", a.activeOpID)
	}
	a.activeOpID = operationID
	var once sync.Once
	return func() {
		once.Do(func() {
			a.mu.Lock()
			defer a.mu.Unlock()
			if a.activeOpID == operationID {
				a.activeOpID = ""
			}
		})
	}, nil
}

// MemoryJournal is an in-memory Journal for testing and development.
type MemoryJournal struct {
	mu     sync.RWMutex
	events []model.Event
}

func NewMemoryJournal() *MemoryJournal {
	return &MemoryJournal{}
}

func (j *MemoryJournal) Append(_ context.Context, event model.Event) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.events = append(j.events, event)
	return nil
}

func (j *MemoryJournal) Events(_ context.Context, configID string) ([]model.Event, error) {
	j.mu.RLock()
	defer j.mu.RUnlock()
	if configID == "" {
		result := make([]model.Event, len(j.events))
		copy(result, j.events)
		return result, nil
	}
	var result []model.Event
	for _, e := range j.events {
		if e.ConfigID == configID {
			result = append(result, e)
		}
	}
	return result, nil
}
