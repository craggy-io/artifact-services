package observability

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
)

func TestReviewNilCollectorSnapshotAndReset(t *testing.T) {
	var c *Collector
	if got := c.Snapshot(SnapshotOptions{}); len(got) != 0 {
		t.Fatalf("nil Snapshot = %v", got)
	}
	c.Reset()
}

func TestReviewCountersSaturate(t *testing.T) {
	collector := &Collector{}
	ctx := With(context.Background(), Options{Collector: collector})
	_, span := Start(ctx, Scope{Layer: "test", Operation: "sat"}, "")
	span.Count("x", math.MaxInt64)
	span.Count("x", math.MaxInt64)
	span.AddBytes(math.MaxInt64)
	span.AddBytes(math.MaxInt64)
	span.End(nil)
	m := collector.Snapshot(SnapshotOptions{})[Scope{Layer: "test", Operation: "sat"}]
	if m.Counters["x"] != math.MaxInt64 || m.Bytes != math.MaxInt64 {
		t.Fatalf("saturation failed: %+v", m)
	}
	collector.Reset()
	if got := collector.Snapshot(SnapshotOptions{}); len(got) != 0 {
		t.Fatalf("after Reset = %v", got)
	}
}

func TestReviewRecordErrorCapped(t *testing.T) {
	events := 0
	ctx := With(context.Background(), Options{Observer: func(Event) { events++ }})
	_, span := Start(ctx, Scope{Layer: "test", Operation: "err"}, "")
	base := events
	for i := 0; i < 100; i++ {
		span.RecordError(errors.New("boom"))
	}
	if got := events - base; got > maxRecordedErrors {
		t.Fatalf("RecordError emitted %d times", got)
	}
	span.End(nil)
}

func TestReviewLabelCapsBoundRetention(t *testing.T) {
	collector := &Collector{}
	ctx := With(context.Background(), Options{Collector: collector})
	_, span := Start(ctx, Scope{Layer: "test", Operation: "labels"}, "")
	for i := 0; i < maxDistinctCounters+100; i++ {
		span.Count(fmt.Sprintf("c%d", i), 1)
	}
	span.End(nil)
	m := collector.Snapshot(SnapshotOptions{})[Scope{Layer: "test", Operation: "labels"}]
	if len(m.Counters) > maxDistinctCounters {
		t.Fatalf("retained %d counters", len(m.Counters))
	}
	for i := 0; i < maxBuckets+10; i++ {
		_, s := Start(ctx, Scope{Layer: "test", Operation: fmt.Sprintf("op%d", i)}, "")
		s.End(nil)
	}
	collector.mu.Lock()
	buckets := len(collector.totals)
	collector.mu.Unlock()
	if buckets > maxBuckets {
		t.Fatalf("retained %d buckets", buckets)
	}
}
