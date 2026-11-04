package storage

import (
	"context"
	"errors"
	"fmt"
	"github.com/craggy-io/artifact-services/observability"
	"io"
	"io/fs"
	"strings"
	"sync"
	"testing"
)

func observedContext() (context.Context, *observability.Collector) {
	c := new(observability.Collector)
	return observability.With(context.Background(), observability.Options{Collector: c}), c
}
func TestContextStorageTree(t *testing.T) {
	s, _ := newTestStore(t)
	var events []observability.Event
	ctx := observability.With(context.Background(), observability.Options{Observer: func(e observability.Event) { events = append(events, e) }})
	ctx, parent := observability.Start(ctx, observability.Scope{Layer: "cas", Operation: "put"}, "ab")
	if err := s.Put(ctx, "ab", strings.NewReader("hello")); err != nil {
		t.Fatal(err)
	}
	parent.End(nil)
	if len(events) < 4 {
		t.Fatal(events)
	}
	for _, e := range events {
		if e.Scope.Layer == "storage" && e.ParentID != events[0].ID {
			t.Fatal("missing parent")
		}
	}
}
func TestCancelledOperations(t *testing.T) {
	s, root := newTestStore(t)
	ctx, c := observedContext()
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	if err := s.Put(ctx, "ab", strings.NewReader("x")); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := s.Open(ctx, "ab"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := s.Exists(ctx, "ab"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	for _, m := range c.Snapshot(observability.SnapshotOptions{}) {
		if m.Completed != 1 || m.Errors != 1 || m.Bytes != 0 {
			t.Fatal(m)
		}
	}
	assertNoPending(t, root)
}

type cancellingReader struct{ cancel context.CancelFunc }

func (r cancellingReader) Read(p []byte) (int, error) { r.cancel(); return copy(p, "partial"), nil }
func TestCancellationDuringStream(t *testing.T) {
	s, root := newTestStore(t)
	ctx, c := observedContext()
	ctx, cancel := context.WithCancel(ctx)
	err := s.Put(ctx, "ab", cancellingReader{cancel})
	var outcome *PutError
	if !errors.Is(err, context.Canceled) || !errors.As(err, &outcome) || outcome.Published {
		t.Fatal(err)
	}
	m := c.Snapshot(observability.SnapshotOptions{})[observability.Scope{Layer: "storage", Operation: "put"}]
	if m.Bytes != 7 || m.Counters["published"] != 0 {
		t.Fatal(m)
	}
	if exists, err := s.Exists(context.Background(), "ab"); exists || err != nil {
		t.Fatal(exists, err)
	}
	assertNoPending(t, root)
}

type lifecycleReader struct {
	err, closeErr error
	closes        int
}

func (r *lifecycleReader) Read(p []byte) (int, error) { return copy(p, "abc"), r.err }
func (r *lifecycleReader) Close() error               { r.closes++; return r.closeErr }
func TestReaderLifecycle(t *testing.T) {
	failure := errors.New("read failed")
	closeFailure := errors.New("close failed")
	for _, tc := range []struct {
		name          string
		read          bool
		err, closeErr error
		eof           bool
	}{{"eof", true, io.EOF, nil, true}, {"early", false, nil, nil, false}, {"read and close errors", true, failure, closeFailure, false}, {"eof and close error", true, io.EOF, closeFailure, true}} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, c := observedContext()
			ctx, span := observability.Start(ctx, observability.Scope{Layer: "storage", Operation: "open"}, "ab")
			base := &lifecycleReader{err: tc.err, closeErr: tc.closeErr}
			r := &blobReader{streamReader: streamReader{ctx: ctx, base: base, span: span}, closer: base}
			if tc.read {
				n, err := r.Read(make([]byte, 8))
				if n != 3 || err != tc.err {
					t.Fatal(n, err)
				}
			}
			if c.Snapshot(observability.SnapshotOptions{})[observability.Scope{Layer: "storage", Operation: "open"}].Completed != 0 {
				t.Fatal("premature completion")
			}
			for range 2 {
				if err := r.Close(); err != tc.closeErr {
					t.Fatal(err)
				}
			}
			if base.closes != 1 {
				t.Fatal("double close")
			}
			if _, err := r.Read(make([]byte, 8)); err != io.ErrClosedPipe {
				t.Fatal(err)
			}
			m := c.Snapshot(observability.SnapshotOptions{})[observability.Scope{Layer: "storage", Operation: "open"}]
			if m.Completed != 1 || (m.Counters["eof"] == 1) != tc.eof || (m.Counters["early_closed"] == 1) == tc.eof {
				t.Fatal(m)
			}
			if (m.Errors == 1) != (tc.err == failure || tc.closeErr != nil) {
				t.Fatal(m)
			}
		})
	}
}
func TestPublicationObservation(t *testing.T) {
	s, _ := newTestStore(t)
	ctx, c := observedContext()
	err := s.put(ctx, "ab", strings.NewReader("data"), func(string) error { return fs.ErrPermission })
	var outcome *PutError
	if !errors.As(err, &outcome) || !outcome.Published {
		t.Fatal(err)
	}
	m := c.Snapshot(observability.SnapshotOptions{})[observability.Scope{Layer: "storage", Operation: "put"}]
	if m.Counters["published"] != 1 || m.Errors != 1 || m.Bytes != 4 {
		t.Fatal(m)
	}
	// TempDir owns and cleans the intentionally retained temporary link.
}
func TestReadCancellationAndPostPublicationCancellation(t *testing.T) {
	s, _ := newTestStore(t)
	ctx, c := observedContext()
	ctx, cancel := context.WithCancel(ctx)
	// Cancellation triggered only after publication must not undo its outcome.
	ctx = observability.With(ctx, observability.Options{Collector: c, Observer: func(e observability.Event) {
		if e.Counters["published"] == 1 {
			cancel()
		}
	}})
	if err := s.Put(ctx, "ab", strings.NewReader("data")); err != nil {
		t.Fatal(err)
	}
	readCtx, readCancel := context.WithCancel(context.Background())
	r, err := s.Open(readCtx, "ab")
	if err != nil {
		t.Fatal(err)
	}
	readCancel()
	if _, err := r.Read(make([]byte, 8)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentStorageChildren(t *testing.T) {
	s, _ := newTestStore(t)
	c := new(observability.Collector)
	var mu sync.Mutex
	var events []observability.Event
	ctx := observability.With(context.Background(), observability.Options{Collector: c, Observer: func(e observability.Event) {
		_ = c.Snapshot(observability.SnapshotOptions{})
		mu.Lock()
		defer mu.Unlock()
		events = append(events, e)
	}})
	ctx, parent := observability.Start(ctx, observability.Scope{Layer: "application", Operation: "batch"}, "batch")
	var wg sync.WaitGroup
	for i := range 12 {
		wg.Go(func() {
			key := fmt.Sprintf("%04x", i)
			if err := s.Put(ctx, key, strings.NewReader("payload")); err != nil {
				t.Error(err)
				return
			}
			r, err := s.Open(ctx, key)
			if err != nil {
				t.Error(err)
				return
			}
			if _, err := io.Copy(io.Discard, r); err != nil {
				t.Error(err)
			}
			if err := r.Close(); err != nil {
				t.Error(err)
			}
			if ok, err := s.Exists(ctx, key); !ok || err != nil {
				t.Error(ok, err)
			}
		})
	}
	wg.Wait()
	parent.Count("completed_batches", 1)
	parent.End(nil)
	snap := c.Snapshot(observability.SnapshotOptions{})
	for _, op := range []string{"put", "open", "exists"} {
		m := snap[observability.Scope{Layer: "storage", Operation: op}]
		if m.Completed != 12 || m.Errors != 0 {
			t.Fatal(m)
		}
	}
	if snap[observability.Scope{Layer: "storage", Operation: "put"}].Bytes != 84 || snap[observability.Scope{Layer: "application", Operation: "batch"}].Bytes != 0 {
		t.Fatal("bytes rolled up")
	}
	seen := map[uint64]bool{}
	for _, e := range events {
		if e.Scope.Layer == "storage" {
			if e.ParentID != events[0].ID {
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
	if len(seen) != 36 {
		t.Fatal(len(seen))
	}
}

func TestStorageInternalFiltering(t *testing.T) {
	s, _ := newTestStore(t)
	c := new(observability.Collector)
	var mu sync.Mutex
	var events []observability.Event
	ctx := observability.With(context.Background(), observability.Options{Collector: c, Observer: func(e observability.Event) { mu.Lock(); defer mu.Unlock(); events = append(events, e) }})
	ctx, parent := observability.Start(ctx, observability.Scope{Layer: "app", Operation: "batch"}, "batch")
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			callCtx := ctx
			if i%2 == 0 {
				callCtx = observability.WithInternal(callCtx)
			}
			key := fmt.Sprintf("%04x", i)
			if err := s.Put(callCtx, key, strings.NewReader("data")); err != nil {
				t.Error(err)
				return
			}
			r, err := s.Open(callCtx, key)
			if err != nil {
				t.Error(err)
				return
			}
			if _, err := io.Copy(io.Discard, r); err != nil {
				t.Error(err)
			}
			if err := r.Close(); err != nil {
				t.Error(err)
			}
			if ok, err := s.Exists(callCtx, key); !ok || err != nil {
				t.Error(ok, err)
			}
		})
	}
	wg.Wait()
	parent.End(nil)
	visible := c.Snapshot(observability.SnapshotOptions{})
	all := c.Snapshot(observability.SnapshotOptions{IncludeInternal: true})
	for _, op := range []string{"put", "open", "exists"} {
		scope := observability.Scope{Layer: "storage", Operation: op}
		if visible[scope].Completed != 4 || all[scope].Completed != 8 {
			t.Fatal(visible, all)
		}
	}
	put := observability.Scope{Layer: "storage", Operation: "put"}
	if visible[put].Bytes != 16 || all[put].Bytes != 32 || visible[put].Counters["published"] != 4 || all[put].Counters["published"] != 8 {
		t.Fatal(visible, all)
	}
	if visible[observability.Scope{Layer: "app", Operation: "batch"}].Bytes != 0 {
		t.Fatal("rolled up bytes")
	}
	finals := 0
	for _, e := range events {
		if e.Category != observability.User {
			t.Fatal("internal event")
		}
		if e.Scope.Layer == "storage" {
			if e.ParentID != events[0].ID {
				t.Fatal("lost parent")
			}
			if e.Final {
				finals++
			}
		}
	}
	if finals != 12 {
		t.Fatal(finals)
	}
	// Ordinary unclassified calls remain visible, regardless of key/name.
	if err := s.Put(ctx, "ffff", strings.NewReader("manifest-looking payload")); err != nil {
		t.Fatal(err)
	}
	if c.Snapshot(observability.SnapshotOptions{})[put].Completed != 5 {
		t.Fatal("direct storage hidden")
	}
}
