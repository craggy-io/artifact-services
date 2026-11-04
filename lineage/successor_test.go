package lineage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/craggy-io/artifact-services/artifacts"
	"github.com/craggy-io/artifact-services/observability"
	"github.com/craggy-io/artifact-services/provenance"
)

// reopen replaces the fixture's service with a new one over the same root.
func (f *fixture) reopen(options Options) {
	f.t.Helper()
	if err := f.service.Close(); err != nil {
		f.t.Fatal(err)
	}
	options.Root = f.root
	service, err := New(f.artifacts, options)
	if err != nil {
		f.t.Fatal(err)
	}
	f.service = service
}

func TestSequencesAreExactSurviveReopenAndStampRecords(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	if seq, err := f.service.Sequence(ctx); err != nil || seq != 0 {
		t.Fatalf("sequence = %d err = %v", seq, err)
	}
	for i := range 5 {
		f.record(Record{ID: ID(fmt.Sprintf("a%d", i)), Activity: &Activity{Kind: "k"}})
		if seq, _ := f.service.Sequence(ctx); seq != uint64(i+1) {
			t.Fatalf("after %d records sequence = %d", i+1, seq)
		}
	}
	// A replayed record is not a commit.
	f.record(Record{ID: "a0", Activity: &Activity{Kind: "k"}})
	if seq, _ := f.service.Sequence(ctx); seq != 5 {
		t.Fatalf("replay advanced the sequence to %d", seq)
	}
	f.reopen(Options{})
	if seq, _ := f.service.Sequence(ctx); seq != 5 {
		t.Fatalf("after reopen sequence = %d", seq)
	}
	got, err := f.service.Get(ctx, "a3")
	if err != nil || got.Sequence != 4 {
		t.Fatalf("a3 = %+v err = %v", got, err)
	}
	page, err := f.service.List(ctx, Query{Limit: 2})
	if err != nil || page.Records[1].Sequence == 0 {
		t.Fatalf("list = %+v err = %v", page, err)
	}
}

func TestLimitsComeFromOptions(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	f.reopen(Options{MaxRecords: 2})
	f.record(Record{ID: "a", Activity: &Activity{Kind: "k"}})
	f.record(Record{ID: "b", Activity: &Activity{Kind: "k"}})
	if _, err := f.service.Record(ctx, Record{ID: "c", Activity: &Activity{Kind: "k"}}); !errors.Is(err, ErrLimit) {
		t.Fatalf("over MaxRecords = %v", err)
	}
	// Raising the limit past what is stored needs only a reopen.
	f.reopen(Options{MaxRecords: 3})
	f.record(Record{ID: "c", Activity: &Activity{Kind: "k"}})
}

func TestRecordsPastTheOldDefaultDoNotStayResident(t *testing.T) {
	if testing.Short() {
		t.Skip("writes many records")
	}
	f := setup(t)
	ctx := context.Background()
	f.reopen(Options{MaxRecords: 200000, Relaxed: true})
	const count = 101000
	var wg sync.WaitGroup
	next := atomic.Int64{}
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := next.Add(1)
				if i > count {
					return
				}
				if _, err := f.service.Record(ctx, Record{ID: ID(fmt.Sprintf("r%d", i)), Activity: &Activity{Kind: "k"}}); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	f.reopen(Options{MaxRecords: 200000, Relaxed: true})
	if seq, _ := f.service.Sequence(ctx); seq != count {
		t.Fatalf("sequence = %d", seq)
	}
	if _, err := f.service.Get(ctx, "r100500"); err != nil {
		t.Fatal(err)
	}
}

func TestEveryReferenceFormRecordsAndQueries(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	artifact := f.artifact(ctx, "dir/file", "0123456789")
	m, err := f.artifacts.Get(ctx, artifact)
	if err != nil {
		t.Fatal(err)
	}
	occurrence, err := f.artifacts.RecordOccurrence(ctx, artifacts.OccurrenceRequest{ID: "occ-1", Artifact: artifact, Name: "n"})
	if err != nil {
		t.Fatal(err)
	}
	assertionExists := func(_ context.Context, id string) error {
		if id != "as-1" {
			return ErrNotFound
		}
		return nil
	}
	f.reopen(Options{ValidateAssertion: assertionExists, ValidateSelector: func(context.Context, provenance.Ref) error { return nil }})
	f.record(Record{ID: "rev", Revision: &Revision{Artifact: artifact}})
	f.record(Record{ID: "act", Activity: &Activity{Kind: "k"}})
	digest := string(m.Entries[0].Content)
	schema := string(artifact)
	valid := []provenance.Ref{
		{Artifact: string(artifact)},
		{Revision: "rev"},
		{Occurrence: string(occurrence.ID)},
		{Digest: digest},
		{Digest: digest, Offset: 2, Length: 3},
		{Artifact: string(artifact), Path: "dir/file"},
		{Occurrence: string(occurrence.ID), Path: "dir/file"},
		{Artifact: string(artifact), Path: "dir/file", Offset: 2, Length: 3},
		{Artifact: string(artifact), Revision: "rev", Row: "row-1"},
		{Artifact: string(artifact), Revision: "rev", Schema: schema, Row: "row-1", Column: "column-1"},
		{Assertion: "as-1"},
		{Activity: "act"},
		{URL: "https://example.test/f", Retrieval: "etag"},
	}
	for i, ref := range valid {
		id := ID(fmt.Sprintf("edge-%d", i))
		f.record(Record{ID: id, Relationship: &Relationship{Kind: "uses", From: provenance.Ref{Artifact: string(artifact)}, To: ref}})
		page, err := f.service.List(ctx, Query{Ref: &ref, Role: "to", RelationshipKind: "uses"})
		if err != nil {
			t.Errorf("query form %+v: %v", ref, err)
			continue
		}
		found := false
		for _, r := range page.Records {
			found = found || r.ID == id
		}
		if !found && ref.Row == "" && ref.Column == "" && ref.Revision == "" {
			t.Errorf("form %+v not found by query: %+v", ref, page.Records)
		}
	}
	// Traversal reaches a non-artifact form.
	result, err := f.service.Traverse(ctx, TraversalQuery{Root: provenance.Ref{Artifact: string(artifact)}, MaxDepth: 1, Kinds: []string{"uses"}})
	if err != nil {
		t.Fatal(err)
	}
	reached := map[string]bool{}
	for _, e := range result.Edges {
		reached[e.To.Form()] = true
	}
	for _, form := range []string{provenance.FormContent, provenance.FormOccurrence, provenance.FormAssertion, provenance.FormActivity, provenance.FormExternal, provenance.FormPath} {
		if !reached[form] {
			t.Errorf("traversal did not reach form %s: %v", form, reached)
		}
	}
	invalid := map[string]provenance.Ref{
		"range past end":     {Digest: digest, Offset: 8, Length: 5},
		"path missing":       {Artifact: string(artifact), Path: "absent"},
		"directory range":    {Artifact: string(artifact), Path: "dir", Offset: 0, Length: 1},
		"mixed families":     {Digest: digest, Artifact: string(artifact)},
		"url no retrieval":   {URL: "https://example.test/f"},
		"unknown occurrence": {Occurrence: "nope"},
		"unknown assertion":  {Assertion: "nope"},
		"unknown activity":   {Activity: "nope"},
		"future version":     {Version: provenance.GrammarVersion + 1, Artifact: string(artifact)},
		"row without pins":   {Artifact: string(artifact), Row: "row-1"},
		"digest not sha":     {Digest: "md5:abc"},
	}
	for name, ref := range invalid {
		_, err := f.service.Record(ctx, Record{ID: ID("bad-" + strings.ReplaceAll(name, " ", "-")), Relationship: &Relationship{Kind: "uses", From: provenance.Ref{Artifact: string(artifact)}, To: ref}})
		if err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

func TestAssertionFormRejectedWithoutValidator(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	artifact := f.artifact(ctx, "f", "x")
	_, err := f.service.Record(ctx, Record{ID: "e", Relationship: &Relationship{Kind: "uses", From: provenance.Ref{Artifact: string(artifact)}, To: provenance.Ref{Assertion: "as-1"}}})
	if !errors.Is(err, provenance.ErrUnsupportedForm) {
		t.Fatalf("no validator = %v, must not be stored unchecked", err)
	}
}

func TestOccurrencesKeepLineageSeparate(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	artifact := f.artifact(ctx, "f", "same bytes")
	for _, id := range []artifacts.OccurrenceID{"one", "two"} {
		if _, err := f.artifacts.RecordOccurrence(ctx, artifacts.OccurrenceRequest{ID: id, Artifact: artifact}); err != nil {
			t.Fatal(err)
		}
	}
	f.record(Record{ID: "about-one", Relationship: &Relationship{Kind: "from", From: provenance.Ref{Occurrence: "one"}, To: provenance.Ref{URL: "https://x", Retrieval: "r"}}})
	two := provenance.Ref{Occurrence: "two"}
	page, err := f.service.List(ctx, Query{Ref: &two})
	if err != nil || len(page.Records) != 0 {
		t.Fatalf("lineage bled across occurrences: %+v err = %v", page, err)
	}
	one := provenance.Ref{Occurrence: "one"}
	page, err = f.service.List(ctx, Query{Ref: &one})
	if err != nil || len(page.Records) != 1 {
		t.Fatalf("one = %+v err = %v", page, err)
	}
}

func metadata(key, value string) map[string]json.RawMessage {
	raw, _ := json.Marshal(value)
	return map[string]json.RawMessage{key: raw}
}

func TestIndexedMetadataLookup(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	options := Options{IndexedMetadata: []string{"derivation"}, Relaxed: true}
	// A root's indexed keys are fixed by its config: use a fresh root.
	if err := f.service.Close(); err != nil {
		t.Fatal(err)
	}
	f.root = filepath.Join(t.TempDir(), "indexed")
	options.Root = f.root
	service, err := New(f.artifacts, options)
	if err != nil {
		t.Fatal(err)
	}
	f.service = service
	const count = 5000
	for i := range count {
		f.record(Record{ID: ID(fmt.Sprintf("a%d", i)), Activity: &Activity{Kind: "run", Metadata: metadata("derivation", fmt.Sprintf("key-%d", i%1000))}})
	}
	lookup := func(want int) {
		t.Helper()
		observed := &observability.Collector{}
		page, err := f.service.List(observability.With(ctx, observability.Options{Collector: observed}), Query{ActivityKind: "run", Metadata: metadata("derivation", "key-7")})
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Records) != want {
			t.Fatalf("records = %d, want %d", len(page.Records), want)
		}
		for _, r := range page.Records {
			if string(r.Activity.Metadata["derivation"]) != `"key-7"` {
				t.Fatalf("wrong record %+v", r)
			}
		}
		for scope, m := range observed.Snapshot(observability.SnapshotOptions{}) {
			if scope.Layer == "lineage" && scope.Operation == "list" && m.Counters["scanned"] > int64(want) {
				t.Fatalf("index lookup scanned %d records for %d hits", m.Counters["scanned"], want)
			}
		}
	}
	lookup(5)
	f.reopen(options)
	lookup(5)
	if _, err := f.service.List(ctx, Query{Metadata: metadata("unindexed", "x")}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unindexed key = %v", err)
	}
	options.IndexedMetadata = []string{"other"}
	if err := f.service.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := New(f.artifacts, options); err == nil {
		t.Fatal("changed index policy accepted for an existing root")
	}
	options.IndexedMetadata = []string{"derivation"}
	if f.service, err = New(f.artifacts, options); err != nil {
		t.Fatal(err)
	}
}

func TestGuardedHistoryRaceHasOneWinner(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	artifact := f.artifact(ctx, "f", "x")
	if _, err := f.service.Head(ctx, "h"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("head of nothing = %v", err)
	}
	if _, err := f.service.Append(ctx, AppendRevision{ID: "h1", History: "h", Artifact: artifact}); err != nil {
		t.Fatal(err)
	}
	const racers = 50
	var wins, conflicts atomic.Int64
	var winner atomic.Value
	var wg sync.WaitGroup
	for i := range racers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := ID(fmt.Sprintf("h2-%d", i))
			_, err := f.service.Append(ctx, AppendRevision{ID: id, History: "h", Expected: "h1", Artifact: artifact})
			var conflict *HeadConflictError
			switch {
			case err == nil:
				wins.Add(1)
				winner.Store(id)
			case errors.Is(err, ErrHeadConflict) && errors.Is(err, ErrConflict) && errors.As(err, &conflict):
				conflicts.Add(1)
			default:
				t.Errorf("append = %v", err)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 || conflicts.Load() != racers-1 {
		t.Fatalf("wins=%d conflicts=%d", wins.Load(), conflicts.Load())
	}
	f.reopen(Options{})
	head, err := f.service.Head(ctx, "h")
	if err != nil || head != winner.Load().(ID) {
		t.Fatalf("head after reopen = %s err = %v, winner %v", head, err, winner.Load())
	}
	// A retry of the winning append reuses even though it is no longer at the head.
	if _, err := f.service.Append(ctx, AppendRevision{ID: "h3", History: "h", Expected: head, Artifact: artifact}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.Append(ctx, AppendRevision{ID: head, History: "h", Expected: "h1", Artifact: artifact}); err != nil {
		t.Fatalf("replayed append = %v", err)
	}
	// Starting an existing history, or a stale expected head, conflicts.
	if _, err := f.service.Append(ctx, AppendRevision{ID: "x", History: "h", Artifact: artifact}); !errors.Is(err, ErrHeadConflict) {
		t.Fatalf("restart = %v", err)
	}
	// HeadAt answers for a past sequence.
	h1, _ := f.service.Get(ctx, "h1")
	if at, err := f.service.HeadAt(ctx, "h", h1.Sequence); err != nil || at != "h1" {
		t.Fatalf("head at %d = %s err = %v", h1.Sequence, at, err)
	}
	if _, err := f.service.HeadAt(ctx, "h", h1.Sequence-1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("before the first revision = %v", err)
	}
	// Histories are independent, and unguarded revisions still branch freely.
	if _, err := f.service.Append(ctx, AppendRevision{ID: "other", History: "other-history", Artifact: artifact}); err != nil {
		t.Fatal(err)
	}
	f.record(Record{ID: "free-1", Revision: &Revision{Artifact: artifact, Parents: []ID{"h1"}}})
	f.record(Record{ID: "free-2", Revision: &Revision{Artifact: artifact, Parents: []ID{"h1"}}})
	if page, err := f.service.List(ctx, Query{History: "other-history"}); err != nil || len(page.Records) != 1 {
		t.Fatalf("by history = %+v err = %v", page, err)
	}
}

// TestChangeCursorResumes is qualification scenario 7.
func TestChangeCursorResumes(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	artifact := f.artifact(ctx, "f", "x")
	f.reopen(Options{Relaxed: true})
	f.record(Record{ID: "first", Activity: &Activity{Kind: "k"}})
	stored, err := f.service.Sequence(ctx)
	if err != nil {
		t.Fatal(err)
	}
	type written struct{ id, class string }
	var want []written
	for i := range 1000 {
		id := fmt.Sprintf("m%d", i)
		switch i % 3 {
		case 0:
			f.record(Record{ID: ID(id), Activity: &Activity{Kind: "k", Actor: "someone"}})
			want = append(want, written{id, "activity"})
		case 1:
			f.record(Record{ID: ID(id), Relationship: &Relationship{Kind: "r", From: provenance.Ref{Artifact: string(artifact)}, To: provenance.Ref{URL: "u", Retrieval: id}}})
			want = append(want, written{id, "relationship"})
		case 2:
			f.record(Record{ID: ID(id), Revision: &Revision{Artifact: artifact}})
			want = append(want, written{id, "revision"})
		}
	}
	f.reopen(Options{Relaxed: true})
	position := stored
	got := []provenance.Change{}
	for {
		page, err := f.service.Changes(ctx, provenance.ChangeQuery{After: position, Limit: 64})
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, page.Changes...)
		position = page.Last
		if len(page.Changes) < 64 {
			break
		}
	}
	if len(got) != len(want) {
		t.Fatalf("resumed %d changes, want %d", len(got), len(want))
	}
	for i, c := range got {
		if c.ID != want[i].id || c.Class != want[i].class || c.Sequence != stored+uint64(i)+1 {
			t.Fatalf("change %d = %+v, want %+v", i, c, want[i])
		}
	}
	if _, err := f.service.Changes(ctx, provenance.ChangeQuery{Limit: 5000}); !errors.Is(err, provenance.ErrChangeLimit) {
		t.Fatalf("oversized page = %v", err)
	}
}

func TestOnCommitFiresOnlyForCommits(t *testing.T) {
	f := setup(t)
	woken := make(chan uint64, 16)
	stop := f.service.OnCommit(func(sequence uint64) { woken <- sequence })
	defer stop()
	f.record(Record{ID: "a", Activity: &Activity{Kind: "k"}})
	if got := <-woken; got != 1 {
		t.Fatalf("woken with %d", got)
	}
	f.record(Record{ID: "a", Activity: &Activity{Kind: "k"}}) // Replay.
	if _, err := f.service.Record(context.Background(), Record{ID: "a", Activity: &Activity{Kind: "different"}}); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflict = %v", err)
	}
	f.record(Record{ID: "b", Activity: &Activity{Kind: "k"}})
	if got := <-woken; got != 2 {
		t.Fatalf("woken with %d, a replay or conflict must not notify", got)
	}
}
