package observability

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestNoObservation(t *testing.T) {
	ctx := context.Background()
	derived, s := Start(ctx, Scope{"x", "y"}, "")
	if derived != ctx || s != nil {
		t.Fatal("not a no-op")
	}
	s.AddBytes(1)
	s.Count("x", 1)
	s.RecordError(errors.New("x"))
	s.End(nil)
}
func TestInheritanceAndConcurrentChildren(t *testing.T) {
	c := new(Collector)
	var mu sync.Mutex
	var events []Event
	ctx := With(context.Background(), Options{Collector: c, Observer: func(e Event) {
		_ = c.Snapshot(SnapshotOptions{})
		mu.Lock()
		defer mu.Unlock()
		events = append(events, e)
	}})
	ctx, parent := Start(ctx, Scope{"artifacts", "publish"}, "manifest")
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			_, child := Start(context.WithValue(ctx, "test", true), Scope{"cas", "put"}, "digest")
			child.AddBytes(5)
			child.Count("dedup", 1)
			child.End(nil)
			child.End(nil)
		})
	}
	wg.Wait()
	parent.Count("completed_artifacts", 1)
	parent.End(nil)
	snap := c.Snapshot(SnapshotOptions{})
	if snap[Scope{"cas", "put"}].Bytes != 100 || snap[Scope{"artifacts", "publish"}].Bytes != 0 {
		t.Fatal(snap)
	}
	if snap[Scope{"cas", "put"}].Completed != 20 || snap[Scope{"artifacts", "publish"}].Counters["completed_artifacts"] != 1 {
		t.Fatal(snap)
	}
	id := events[0].ID
	seen := map[uint64]bool{}
	for _, e := range events {
		if e.Scope.Layer == "cas" {
			if e.ParentID != id {
				t.Fatal("wrong parent")
			}
			if e.Final {
				if seen[e.ID] {
					t.Fatal("duplicate final")
				}
				seen[e.ID] = true
			}
		}
	}
	snap[Scope{"cas", "put"}].Counters["dedup"] = 999
	if c.Snapshot(SnapshotOptions{})[Scope{"cas", "put"}].Counters["dedup"] != 20 {
		t.Fatal("snapshot shares maps")
	}
	disabled := With(ctx, Options{})
	_, s := Start(disabled, Scope{"x", "y"}, "")
	if s != nil {
		t.Fatal("subtree not disabled")
	}
}
func TestThrottleTotalsAndErrors(t *testing.T) {
	var events []Event
	c := new(Collector)
	ctx := With(context.Background(), Options{Collector: c, ProgressInterval: time.Hour, Total: func(Scope, string) (int64, bool) { return 10, true }, Observer: func(e Event) { events = append(events, e) }})
	_, s := Start(ctx, Scope{"test", "work"}, "key")
	s.AddBytes(2)
	if len(events) != 1 {
		t.Fatal("not throttled")
	}
	s.last = time.Now().Add(-2 * time.Hour)
	s.AddBytes(3)
	if events[1].Bytes != 5 {
		t.Fatal("wrong progress")
	}
	failure := errors.New("failed")
	s.RecordError(failure)
	s.End(nil)
	s.End(nil)
	s.AddBytes(99)
	last := events[len(events)-1]
	if !last.Final || last.Bytes != 5 || last.Total != 10 || !errors.Is(last.Err, failure) {
		t.Fatal(last)
	}
	if c.Snapshot(SnapshotOptions{})[Scope{"test", "work"}].Errors != 1 {
		t.Fatal("error count")
	}
}
