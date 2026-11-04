package observability

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestInternalSelectionAndStickyInheritance(t *testing.T) {
	c := new(Collector)
	var events []Event
	options := Options{Collector: c, Observer: func(e Event) { _ = c.Snapshot(SnapshotOptions{}); events = append(events, e) }, ProgressInterval: time.Hour}
	root := With(context.Background(), options)
	ctx, parent := Start(root, Scope{"app", "request"}, "request")
	parent.AddBytes(10)
	internalCtx := WithInternal(ctx)
	// Reconfiguration and context derivation must not reset classification.
	internalCtx = With(context.WithValue(internalCtx, "test", true), options)
	internalCtx, hidden := Start(internalCtx, Scope{"store", "put"}, "manifest")
	hidden.AddBytes(100)
	hidden.Count("published", 1)
	_, child := Start(internalCtx, Scope{"store", "put"}, "index")
	child.AddBytes(200)
	child.Count("published", 1)
	child.End(nil)
	child.End(nil)
	failure := errors.New("internal failure")
	hidden.End(failure)
	// The parent still reports the real failure and inclusive duration.
	parent.start = time.Now().Add(-time.Second)
	parent.End(failure)
	for _, e := range events {
		if e.Category != User || e.Scope.Layer != "app" {
			t.Fatalf("internal leak: %+v", e)
		}
	}
	last := events[len(events)-1]
	if !last.Final || !errors.Is(last.Err, failure) || last.Elapsed < time.Second || last.Bytes != 10 {
		t.Fatal(last)
	}
	visible := c.Snapshot(SnapshotOptions{})
	if len(visible) != 1 || visible[Scope{"app", "request"}].Errors != 1 {
		t.Fatal(visible)
	}
	all := c.Snapshot(SnapshotOptions{IncludeInternal: true})
	if m := all[Scope{"store", "put"}]; m.Completed != 2 || m.Bytes != 300 || m.Errors != 1 || m.Counters["published"] != 2 {
		t.Fatal(m)
	}
	// Enable future internal events, retaining the original trace linkage.
	options.IncludeInternal = true
	diagnosticCtx := With(internalCtx, options)
	_, diagnostic := Start(diagnosticCtx, Scope{"store", "get"}, "support")
	diagnostic.End(nil)
	e := events[len(events)-1]
	if e.Category != Internal || e.ParentID != hidden.event.ID {
		t.Fatal(e)
	}
	for _, e := range events {
		if e.ID == hidden.event.ID || e.ID == child.event.ID {
			t.Fatal("old events replayed")
		}
	}
	// User sibling retains default category and does not inherit diagnostic opt-in.
	_, sibling := Start(ctx, Scope{"store", "put"}, "payload")
	sibling.AddBytes(7)
	sibling.End(nil)
	if c.Snapshot(SnapshotOptions{})[Scope{"store", "put"}].Bytes != 7 {
		t.Fatal("context leaked")
	}
	if c.Snapshot(SnapshotOptions{IncludeInternal: true})[Scope{"store", "put"}].Bytes != 307 {
		t.Fatal("bucket merge")
	}
}

func TestClassificationBeforeConfigurationAndNoObservation(t *testing.T) {
	internal := WithInternal(context.Background())
	child, span := Start(internal, Scope{"store", "put"}, "support")
	if child != internal || span != nil {
		t.Fatal("disabled observation allocated span")
	}
	var events []Event
	configured := With(child, Options{IncludeInternal: true, Observer: func(e Event) { events = append(events, e) }})
	_, span = Start(configured, Scope{"store", "put"}, "support")
	span.End(nil)
	if len(events) != 2 || events[0].Category != Internal {
		t.Fatal(events)
	}
	disabled := With(configured, Options{})
	_, span = Start(disabled, Scope{"x", "y"}, "")
	if span != nil {
		t.Fatal("not disabled")
	}
	_, span = Start(With(disabled, Options{Observer: func(Event) { t.Fatal("internal reset") }}), Scope{"x", "y"}, "")
	span.End(nil)
}

func TestInternalDiagnosticProgressAndFinal(t *testing.T) {
	var events []Event
	ctx := WithInternal(With(context.Background(), Options{IncludeInternal: true, Observer: func(e Event) { events = append(events, e) }, ProgressInterval: time.Hour}))
	_, span := Start(ctx, Scope{"x", "write"}, "support")
	span.AddBytes(2)
	if len(events) != 1 {
		t.Fatal("not throttled")
	}
	span.last = time.Now().Add(-2 * time.Hour)
	span.AddBytes(3)
	span.End(nil)
	span.End(nil)
	if len(events) != 3 || events[1].Bytes != 5 || !events[2].Final || events[2].Category != Internal {
		t.Fatal(events)
	}
}
