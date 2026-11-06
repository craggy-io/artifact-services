package watch_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/craggy-io/artifact-services/adapters/testkit"
	"github.com/craggy-io/artifact-services/adapters/typed"
	"github.com/craggy-io/artifact-services/adapters/watch"
	"github.com/craggy-io/artifact-services/artifacts"
	"github.com/craggy-io/artifact-services/assertions"
	"github.com/craggy-io/artifact-services/lineage"
	"github.com/craggy-io/artifact-services/provenance"
)

type note struct {
	Text string `json:"text"`
}

// log collects reported batches; its handler runs on one goroutine at a time.
type log struct {
	mu      sync.Mutex
	batches []watch.Batch
	seen    map[string]map[uint64]int
}

func newLog() *log { return &log{seen: map[string]map[uint64]int{}} }

func (l *log) handle(_ context.Context, b watch.Batch) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.batches = append(l.batches, b)
	if l.seen[b.Journal] == nil {
		l.seen[b.Journal] = map[uint64]int{}
	}
	for _, c := range b.Changes {
		l.seen[b.Journal][c.Sequence]++
	}
	return nil
}

// requireEach fails unless every sequence 1..tail of each journal was reported
// exactly once, in increasing order.
func (l *log) requireEach(t *testing.T, tail watch.Position) {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	last := map[string]uint64{}
	for _, b := range l.batches {
		for _, c := range b.Changes {
			if c.Sequence <= last[b.Journal] {
				t.Fatalf("%s: sequence %d after %d", b.Journal, c.Sequence, last[b.Journal])
			}
			last[b.Journal] = c.Sequence
		}
	}
	for journal, n := range map[string]uint64{watch.Artifacts: tail.Artifacts, watch.Lineage: tail.Lineage, watch.Assertions: tail.Assertions} {
		if uint64(len(l.seen[journal])) != n {
			t.Fatalf("%s: %d sequences reported, tail %d", journal, len(l.seen[journal]), n)
		}
		for seq, count := range l.seen[journal] {
			if count != 1 || seq < 1 || seq > n {
				t.Fatalf("%s: sequence %d reported %d times (tail %d)", journal, seq, count, n)
			}
		}
	}
}

func (l *log) classes(journal string) map[string]int {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := map[string]int{}
	for _, b := range l.batches {
		if b.Journal == journal {
			for _, c := range b.Changes {
				out[c.Class]++
			}
		}
	}
	return out
}

func current(t *testing.T, s *testkit.Stack) watch.Position {
	t.Helper()
	p, err := watch.Current(s.Ctx, watch.Of(s.Stack))
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// caughtUp waits for w to reach the stack's current position.
func caughtUp(t *testing.T, s *testkit.Stack, w *watch.Watch) {
	t.Helper()
	want := current(t, s)
	deadline := time.Now().Add(10 * time.Second)
	for w.Position() != want {
		if time.Now().After(deadline) {
			t.Fatalf("watch at %+v, want %+v", w.Position(), want)
		}
		select {
		case <-w.Done():
			t.Fatalf("watch ended: %v", w.Stop())
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// write commits one record to each journal: an artifact and its occurrence, a
// session's start and end, and a claim about the artifact.
func write(t *testing.T, s *testkit.Stack, kind typed.Kind[note], name string) {
	t.Helper()
	artifact := s.File(name, name)
	if _, err := s.Artifacts.RecordOccurrence(s.Ctx, artifacts.OccurrenceRequest{ID: artifacts.OccurrenceID("occ-" + name), Artifact: artifact, Name: name}); err != nil {
		t.Fatal(err)
	}
	session := s.Session("alice")
	if err := session.End(s.Ctx, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := kind.Claim(s.Ctx, provenance.Ref{Artifact: string(artifact)}, note{Text: name}, typed.WithID(assertions.ID("claim-"+name))); err != nil {
		t.Fatal(err)
	}
}

func defineNote(t *testing.T, s *testkit.Stack) typed.Kind[note] {
	t.Helper()
	kind, err := typed.DefineKind[note](s.Ctx, s.Assertions, typed.KindDefinition{Name: "note", Version: 1})
	if err != nil {
		t.Fatal(err)
	}
	return kind
}

func TestDrainReportsEachJournalInOrderWithClasses(t *testing.T) {
	s := testkit.Open(t)
	kind := defineNote(t, s)
	for i := range 3 {
		write(t, s, kind, fmt.Sprintf("f%d", i))
	}
	l := newLog()
	reached, err := watch.Drain(s.Ctx, watch.Of(s.Stack), watch.Options{IncludeInternal: true, Limit: 2}, l.handle)
	if err != nil {
		t.Fatal(err)
	}
	tail := current(t, s)
	if reached != tail {
		t.Fatalf("drain reached %+v, tail %+v", reached, tail)
	}
	l.requireEach(t, tail)
	if got := l.classes(watch.Artifacts); got[artifacts.ChangeArtifact] != 3 || got[artifacts.ChangeOccurrence] != 3 {
		t.Fatalf("artifact classes = %v", got)
	}
	if got := l.classes(watch.Lineage); got["activity"] == 0 {
		t.Fatalf("lineage classes = %v", got)
	}
	if got := l.classes(watch.Assertions); got[assertions.ChangeKind] != 1 || got[assertions.ChangeAssertion] != 3 {
		t.Fatalf("assertion classes = %v", got)
	}
	// Batches follow dependency order, each carrying the position after it.
	order := map[string]int{watch.Artifacts: 0, watch.Lineage: 1, watch.Assertions: 2}
	for i := 1; i < len(l.batches); i++ {
		if order[l.batches[i].Journal] < order[l.batches[i-1].Journal] {
			t.Fatalf("batch %d from %s after %s", i, l.batches[i].Journal, l.batches[i-1].Journal)
		}
	}
	if last := l.batches[len(l.batches)-1]; last.Position != tail || len(last.Changes) > 2 {
		t.Fatalf("last batch = %+v", last)
	}
	// Draining again from the end reports nothing.
	again := newLog()
	if _, err := watch.Drain(s.Ctx, watch.Of(s.Stack), watch.Options{From: reached}, again.handle); err != nil || len(again.batches) != 0 {
		t.Fatalf("second drain = %+v, %v", again.batches, err)
	}
}

func TestStartFromCurrentReportsOnlyLaterCommits(t *testing.T) {
	s := testkit.Open(t)
	kind := defineNote(t, s)
	write(t, s, kind, "before")
	from := current(t, s)
	l := newLog()
	w := watch.Start(s.Ctx, watch.Of(s.Stack), watch.Options{From: from}, l.handle)
	defer w.Stop()
	write(t, s, kind, "after")
	caughtUp(t, s, w)
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, b := range l.batches {
		for _, c := range b.Changes {
			if c.ID == "occ-before" || c.ID == "claim-before" {
				t.Fatalf("reported history: %+v", c)
			}
		}
	}
	if l.seen[watch.Artifacts][from.Artifacts+1] != 1 || l.seen[watch.Assertions][from.Assertions+1] != 1 {
		t.Fatalf("later commits not reported: %+v", l.seen)
	}
}

func TestResumeFromSavedPositionReportsEachCommitOnceAcrossRestart(t *testing.T) {
	s := testkit.Open(t)
	kind := defineNote(t, s)
	l := newLog()
	var saved watch.Position
	save := func(ctx context.Context, b watch.Batch) error {
		if err := l.handle(ctx, b); err != nil {
			return err
		}
		saved = b.Position
		return nil
	}
	o := watch.Options{IncludeInternal: true, Limit: 3}
	w := watch.Start(s.Ctx, watch.Of(s.Stack), o, save)
	for i := range 4 {
		write(t, s, kind, fmt.Sprintf("a%d", i))
	}
	caughtUp(t, s, w)
	if err := w.Stop(); err != nil {
		t.Fatal(err)
	}
	if saved != w.Position() {
		t.Fatalf("saved %+v, watch at %+v", saved, w.Position())
	}

	s.Reopen()
	kind = defineNote(t, s)
	for i := range 4 {
		write(t, s, kind, fmt.Sprintf("b%d", i))
	}
	o.From = saved
	w = watch.Start(s.Ctx, watch.Of(s.Stack), o, save)
	defer w.Stop()
	write(t, s, kind, "c")
	caughtUp(t, s, w)
	l.requireEach(t, current(t, s))
}

func TestConcurrentWritersAreReportedOnceAndAfterWhatTheyReference(t *testing.T) {
	s := testkit.Open(t)
	kind := defineNote(t, s)
	var mu sync.Mutex
	subjectOf := map[string]string{} // claim ID -> artifact ID, filled before the claim commits
	seenArtifact := map[string]bool{}
	var violation atomic.Value
	l := newLog()
	handle := func(ctx context.Context, b watch.Batch) error {
		mu.Lock()
		for _, c := range b.Changes {
			switch {
			case b.Journal == watch.Artifacts && c.Class == artifacts.ChangeArtifact:
				seenArtifact[c.ID] = true
			case b.Journal == watch.Assertions && c.Class == assertions.ChangeAssertion:
				if !seenArtifact[subjectOf[c.ID]] {
					violation.Store(fmt.Sprintf("claim %s reported before artifact %s", c.ID, subjectOf[c.ID]))
				}
			}
		}
		mu.Unlock()
		return l.handle(ctx, b)
	}
	w := watch.Start(s.Ctx, watch.Of(s.Stack), watch.Options{IncludeInternal: true}, handle)
	defer w.Stop()

	const writers, each = 6, 15
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for g := range writers {
		wg.Go(func() {
			for i := range each {
				name := fmt.Sprintf("w%d-%d", g, i)
				content, err := s.CAS.Put(s.Ctx, strings.NewReader(name))
				if err != nil {
					errs <- err
					return
				}
				artifact, err := s.Artifacts.Publish(s.Ctx, artifacts.Manifest{Version: 1, Entries: []artifacts.Entry{{Path: name, Content: content.ID, Size: content.Size}}})
				if err != nil {
					errs <- err
					return
				}
				claim := "claim-" + name
				mu.Lock()
				subjectOf[claim] = string(artifact)
				mu.Unlock()
				if _, err := kind.Claim(s.Ctx, provenance.Ref{Artifact: string(artifact)}, note{Text: name}, typed.WithID(assertions.ID(claim))); err != nil {
					errs <- err
					return
				}
				if _, err := s.Lineage.Record(s.Ctx, lineage.Record{ID: lineage.ID("act-" + name), Activity: &lineage.Activity{Kind: "write", Actor: name}}); err != nil {
					errs <- err
					return
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	caughtUp(t, s, w)
	if v := violation.Load(); v != nil {
		t.Fatal(v)
	}
	l.requireEach(t, current(t, s))
	if got := l.classes(watch.Assertions)[assertions.ChangeAssertion]; got != writers*each {
		t.Fatalf("claims reported = %d", got)
	}
}

func TestHandlerErrorEndsTheWatchWithoutAdvancing(t *testing.T) {
	s := testkit.Open(t)
	kind := defineNote(t, s)
	write(t, s, kind, "x")
	failure := errors.New("cannot store")
	w := watch.Start(s.Ctx, watch.Of(s.Stack), watch.Options{}, func(_ context.Context, b watch.Batch) error {
		if b.Journal == watch.Assertions {
			return failure
		}
		return nil
	})
	select {
	case <-w.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("watch did not end")
	}
	if err := w.Stop(); !errors.Is(err, failure) {
		t.Fatalf("stop = %v", err)
	}
	tail := current(t, s)
	if got := w.Position(); got.Assertions != 0 || got.Artifacts != tail.Artifacts || got.Lineage != tail.Lineage {
		t.Fatalf("position = %+v, tail %+v", got, tail)
	}
}

// counting counts reads of a journal.
type counting struct {
	watch.Journal
	reads *atomic.Int64
}

func (c counting) Changes(ctx context.Context, q provenance.ChangeQuery) (provenance.ChangePage, error) {
	c.reads.Add(1)
	return c.Journal.Changes(ctx, q)
}

func TestIdleWatchDoesNotRead(t *testing.T) {
	s := testkit.Open(t)
	kind := defineNote(t, s)
	var reads atomic.Int64
	j := watch.Of(s.Stack)
	j.Artifacts = counting{j.Artifacts, &reads}
	j.Assertions = counting{j.Assertions, &reads}
	l := newLog()
	w := watch.Start(s.Ctx, j, watch.Options{}, l.handle)
	defer w.Stop()
	write(t, s, kind, "x")
	caughtUp(t, s, w)
	settled := reads.Load()
	time.Sleep(100 * time.Millisecond)
	if got := reads.Load(); got != settled {
		t.Fatalf("idle watch read %d more pages", got-settled)
	}
	write(t, s, kind, "y")
	caughtUp(t, s, w)
	if reads.Load() == settled {
		t.Fatal("commit was not read")
	}
}
