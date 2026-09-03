package observability

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

type panicObserver struct{}

func (panicObserver) Start(context.Context, Activity) (context.Context, Span) { panic("start") }
func (panicObserver) Committed(context.Context, Transition)                   { panic("committed") }
func (panicObserver) Runtime(context.Context, RuntimeSnapshot)                { panic("runtime") }

type panicSpan struct{}

func (panicSpan) Event(string, ...Field) { panic("event") }
func (panicSpan) Error(error, ...Field)  { panic("error") }
func (panicSpan) End(ActivityResult)     { panic("end") }

func TestSafeObserverContainsPanicsAndNil(t *testing.T) {
	ctx := context.WithValue(context.Background(), struct{}{}, "value")
	for _, observer := range []Observer{nil, panicObserver{}} {
		safe := Safe(observer)
		next, span := safe.Start(ctx, Activity{})
		if next != ctx || span == nil {
			t.Fatalf("safe start changed context or returned nil: %#v %#v", next, span)
		}
		span.Event("event")
		span.Error(errors.New("failure"))
		span.End(ActivityResult{})
		safe.Committed(ctx, Transition{})
		safe.Runtime(ctx, RuntimeSnapshot{})
	}
	SafeSpan(panicSpan{}).End(ActivityResult{})
}

type captureSink struct {
	activities chan CompletedActivity
}

func (s *captureSink) Activity(_ context.Context, value CompletedActivity) { s.activities <- value }
func (*captureSink) Committed(context.Context, Transition)                 {}
func (*captureSink) Runtime(context.Context, RuntimeSnapshot)              {}

func TestAsyncObserverBoundsEventsAndSanitizesFields(t *testing.T) {
	sink := &captureSink{activities: make(chan CompletedActivity, 2)}
	observer := NewAsync(sink, 4, 2)
	_, span := observer.Start(context.Background(), Activity{Kind: ActivityReplan})
	span.Event("one", Field{Key: "state", Value: "ready"}, Field{Key: "password", Value: "secret"})
	span.Event("two", Field{Key: "code", Value: "ok"})
	span.Event("three")
	span.End(ActivityResult{Reason: strings.Repeat("x", MaxReasonLength+10)})
	select {
	case activity := <-sink.activities:
		if len(activity.Events) != 2 || len(activity.Events[0].Fields) != 1 || activity.Events[0].Fields[0].Key != "state" {
			t.Fatalf("unexpected sanitized events: %#v", activity.Events)
		}
		if len(activity.Result.Reason) != MaxReasonLength {
			t.Fatalf("reason length = %d", len(activity.Result.Reason))
		}
	case <-time.After(time.Second):
		t.Fatal("activity was not exported")
	}
	if observer.Dropped() != 1 {
		t.Fatalf("dropped = %d, want 1", observer.Dropped())
	}
	_, secretSpan := observer.Start(context.Background(), Activity{Kind: ActivityProviderExecute})
	secretSpan.Error(errors.New(`request https://example.invalid/run?token=top-secret failed: password=hunter2`))
	secretSpan.End(ActivityResult{Reason: `{"authorization":"Bearer abc.def","message":"failed"}`})
	select {
	case activity := <-sink.activities:
		serialized := activity.Result.Reason + activity.Events[0].Error
		for _, secret := range []string{"top-secret", "hunter2", "abc.def"} {
			if strings.Contains(serialized, secret) {
				t.Fatalf("secret %q escaped sanitization: %s", secret, serialized)
			}
		}
	case <-time.After(time.Second):
		t.Fatal("secret activity was not exported")
	}
	if err := observer.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type blockingSink struct {
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (s *blockingSink) block() {
	s.once.Do(func() { close(s.entered) })
	<-s.release
}
func (s *blockingSink) Activity(context.Context, CompletedActivity) { s.block() }
func (s *blockingSink) Committed(context.Context, Transition)       { s.block() }
func (s *blockingSink) Runtime(context.Context, RuntimeSnapshot)    { s.block() }

func TestAsyncObserverBackpressureAndShutdownDeadline(t *testing.T) {
	sink := &blockingSink{entered: make(chan struct{}), release: make(chan struct{})}
	observer := NewAsync(sink, 2, 1)
	observer.Committed(context.Background(), Transition{ID: "first"})
	select {
	case <-sink.entered:
	case <-time.After(time.Second):
		t.Fatal("sink was not entered")
	}
	started := time.Now()
	for i := 0; i < 10_000; i++ {
		observer.Committed(context.Background(), Transition{ID: "saturated"})
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("enqueue blocked for %s", elapsed)
	}
	if observer.Dropped() == 0 || len(observer.queue) > cap(observer.queue) {
		t.Fatalf("bounded queue invariant failed: len=%d cap=%d dropped=%d", len(observer.queue), cap(observer.queue), observer.Dropped())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := observer.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown error = %v", err)
	}
	// Calls racing with or following shutdown are dropped, never panics.
	observer.Committed(context.Background(), Transition{ID: "after-close"})
	close(sink.release)
}

type discardSink struct{}

func (discardSink) Activity(context.Context, CompletedActivity) {}
func (discardSink) Committed(context.Context, Transition)       {}
func (discardSink) Runtime(context.Context, RuntimeSnapshot)    {}

func BenchmarkObserverActivity(b *testing.B) {
	b.Run("noop", func(b *testing.B) {
		observer := Noop()
		for i := 0; i < b.N; i++ {
			_, span := observer.Start(context.Background(), Activity{Kind: ActivityAcceptDesired})
			span.End(ActivityResult{Outcome: "accepted"})
		}
	})
	b.Run("async", func(b *testing.B) {
		observer := NewAsync(discardSink{}, DefaultQueueSize, DefaultMaxEvents)
		b.Cleanup(func() { _ = observer.Shutdown(context.Background()) })
		for i := 0; i < b.N; i++ {
			_, span := observer.Start(context.Background(), Activity{Kind: ActivityAcceptDesired})
			span.End(ActivityResult{Outcome: "accepted"})
		}
	})
}
