package observability

import (
	"context"
	"sync"
	"testing"
)

func TestCategoryFromTracksClassificationWithoutObservation(t *testing.T) {
	root := context.Background()
	internal := WithInternal(root)
	configured := With(internal, Options{Collector: new(Collector)})
	derived, span := Start(configured, Scope{Layer: "catalog", Operation: "register"}, "record")
	span.End(nil)
	disabled := With(derived, Options{})
	canceled, cancel := context.WithCancel(disabled)
	cancel()
	cases := []struct {
		name string
		ctx  context.Context
		want Category
	}{
		{"ordinary", root, User},
		{"user configured", With(root, Options{Collector: new(Collector)}), User},
		{"internal without observer", internal, Internal},
		{"configured internal", configured, Internal},
		{"child scope", derived, Internal},
		{"disabled observation", disabled, Internal},
		{"canceled child", canceled, Internal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := CategoryFrom(tc.ctx); got != tc.want {
				t.Fatalf("category = %v, want %v", got, tc.want)
			}
		})
	}
	if CategoryFrom(root) != User {
		t.Fatal("internal child changed user parent")
	}
	if WithInternal(internal) != internal {
		t.Fatal("reclassifying internal context allocated a new context")
	}
}

func TestCategoryFromAndDisabledScopesAllocateNothing(t *testing.T) {
	for _, ctx := range []context.Context{context.Background(), WithInternal(context.Background())} {
		allocations := testing.AllocsPerRun(100, func() {
			_ = CategoryFrom(ctx)
			child, span := Start(ctx, Scope{Layer: "catalog", Operation: "get"}, "id")
			if child != ctx || span != nil {
				t.Fatal("disabled scope changed context")
			}
		})
		if allocations != 0 {
			t.Fatalf("disabled classification/scope allocated %v times", allocations)
		}
	}
}

func TestConcurrentCategoryConsumersAndIndependentEventValues(t *testing.T) {
	collector := new(Collector)
	options := Options{Collector: collector, IncludeInternal: true, Observer: func(e Event) {
		// Applications may retain or annotate an event without mutating the span or
		// collector. Snapshot during callback must not deadlock.
		if e.Counters != nil {
			e.Counters["registered"] = 999
		}
		_ = collector.Snapshot(SnapshotOptions{IncludeInternal: true})
	}}
	root := With(context.Background(), options)
	internal := WithInternal(root)
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Go(func() {
			ctx, want := root, User
			if i%2 == 0 {
				ctx, want = internal, Internal
			}
			child, span := Start(ctx, Scope{Layer: "catalog", Operation: "register"}, "id")
			if CategoryFrom(child) != want {
				t.Error("classification changed across child")
			}
			span.Count("registered", 1)
			span.End(nil)
		})
	}
	wg.Wait()
	scope := Scope{Layer: "catalog", Operation: "register"}
	if m := collector.Snapshot(SnapshotOptions{})[scope]; m.Completed != 16 || m.Counters["registered"] != 16 {
		t.Fatalf("user metrics: %+v", m)
	}
	if m := collector.Snapshot(SnapshotOptions{IncludeInternal: true})[scope]; m.Completed != 32 || m.Counters["registered"] != 32 {
		t.Fatalf("diagnostic metrics: %+v", m)
	}
}
