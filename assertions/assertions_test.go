package assertions

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
	"github.com/craggy-io/artifact-services/cas"
	"github.com/craggy-io/artifact-services/lineage"
	"github.com/craggy-io/artifact-services/objects"
	"github.com/craggy-io/artifact-services/observability"
	"github.com/craggy-io/artifact-services/provenance"
	"github.com/craggy-io/artifact-services/storage"
)

type fixture struct {
	t        *testing.T
	ctx      context.Context
	root     string
	cas      *cas.Service
	store    *artifacts.Service
	s        *Service
	options  Options
	artifact artifacts.ID
	file     cas.ID
}

func setup(t *testing.T, options Options) *fixture {
	t.Helper()
	root := t.TempDir()
	disk, err := storage.NewFilesystem(filepath.Join(root, "blobs"))
	if err != nil {
		t.Fatal(err)
	}
	content, err := cas.New(disk, cas.Options{})
	if err != nil {
		t.Fatal(err)
	}
	store, err := artifacts.New(content, artifacts.Options{Root: filepath.Join(root, "catalog"), Relaxed: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	f := &fixture{t: t, ctx: context.Background(), root: root, cas: content, store: store, options: options}
	put, err := content.Put(f.ctx, strings.NewReader("payload"))
	if err != nil {
		t.Fatal(err)
	}
	f.file = put.ID
	f.artifact, err = store.Publish(f.ctx, artifacts.Manifest{Version: 1, Entries: []artifacts.Entry{{Path: "f", Content: put.ID, Size: put.Size}}})
	if err != nil {
		t.Fatal(err)
	}
	f.open()
	return f
}

func (f *fixture) open() {
	f.t.Helper()
	options := f.options
	options.Root = filepath.Join(f.root, "assertions")
	options.Relaxed = true
	s, err := New(f.store, options)
	if err != nil {
		f.t.Fatal(err)
	}
	f.s = s
	f.t.Cleanup(func() {
		if err := s.Close(); err != nil {
			f.t.Error(err)
		}
	})
}

func (f *fixture) reopen() {
	f.t.Helper()
	if err := f.s.Close(); err != nil {
		f.t.Fatal(err)
	}
	f.open()
}

var objectShape = objects.Shape{Type: "object", Properties: map[string]objects.Shape{"n": {Type: "integer"}}, Required: []string{"n"}}

func (f *fixture) register(k Kind) KindID {
	f.t.Helper()
	id, err := f.s.Register(f.ctx, k, Writer{})
	if err != nil {
		f.t.Fatal(err)
	}
	return id
}

func (f *fixture) subject() provenance.Ref { return provenance.Ref{Artifact: string(f.artifact)} }

func value(n int) json.RawMessage { return json.RawMessage(fmt.Sprintf(`{"n":%d}`, n)) }

func (f *fixture) record(r Request) ID {
	f.t.Helper()
	if r.Subject == (provenance.Ref{}) {
		r.Subject = f.subject()
	}
	if r.Value == nil {
		r.Value = value(1)
	}
	id, err := f.s.Record(f.ctx, r)
	if err != nil {
		f.t.Fatal(err)
	}
	return id
}

func (f *fixture) list(q Query) []Assertion {
	f.t.Helper()
	page, err := f.s.List(f.ctx, q)
	if err != nil {
		f.t.Fatal(err)
	}
	return page.Assertions
}

func ids(list []Assertion) string {
	parts := []string{}
	for _, a := range list {
		parts = append(parts, string(a.ID))
	}
	return strings.Join(parts, ",")
}

func TestLifecycleReplaceRetractAndHistorySurviveReopen(t *testing.T) {
	f := setup(t, Options{})
	kind := f.register(Kind{Name: "note", Version: 1, Shape: objectShape})
	f.record(Request{ID: "a1", Kind: kind})
	f.record(Request{ID: "a2", Kind: kind, Replaces: "a1", Value: value(2)})
	f.record(Request{ID: "b1", Kind: kind})
	if err := f.s.Retract(f.ctx, Retraction{Assertion: "b1", Actor: "someone"}); err != nil {
		t.Fatal(err)
	}
	check := func() {
		t.Helper()
		if got := ids(f.list(Query{Subject: ptr(f.subject())})); got != "a2" {
			t.Fatalf("current by subject = %s", got)
		}
		if got := ids(f.list(Query{Kind: kind})); got != "a2" {
			t.Fatalf("current by kind = %s", got)
		}
		if got := ids(f.list(Query{Kind: kind, IncludeEnded: true})); got != "a1,a2,b1" {
			t.Fatalf("all by kind = %s", got)
		}
		replaced, err := f.s.Get(f.ctx, "a1")
		if err != nil || replaced.Current || replaced.EndedBy != "a2" || replaced.Retracted {
			t.Fatalf("a1 = %+v err = %v", replaced, err)
		}
		retracted, err := f.s.Get(f.ctx, "b1")
		if err != nil || retracted.Current || !retracted.Retracted {
			t.Fatalf("b1 = %+v err = %v", retracted, err)
		}
		chain, err := f.s.History(f.ctx, "a1")
		if err != nil || ids(chain) != "a1,a2" {
			t.Fatalf("history = %s err = %v", ids(chain), err)
		}
	}
	check()
	before, _ := f.s.Sequence(f.ctx)
	f.reopen()
	after, _ := f.s.Sequence(f.ctx)
	if before != after || before != 5 {
		t.Fatalf("sequence %d then %d, want 5", before, after)
	}
	check()
}

func ptr[T any](v T) *T { return &v }

func TestReplacementCannotFork(t *testing.T) {
	f := setup(t, Options{})
	kind := f.register(Kind{Name: "note", Version: 1, Shape: objectShape})
	f.record(Request{ID: "a", Kind: kind})
	f.record(Request{ID: "b", Kind: kind, Replaces: "a"})
	_, err := f.s.Record(f.ctx, Request{ID: "c", Kind: kind, Subject: f.subject(), Value: value(1), Replaces: "a"})
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("second replacement of a = %v", err)
	}
	if err := f.s.Retract(f.ctx, Retraction{Assertion: "a"}); !errors.Is(err, ErrConflict) {
		t.Fatalf("retract of replaced = %v", err)
	}
	if _, err := f.s.Record(f.ctx, Request{ID: "d", Kind: kind, Subject: f.subject(), Value: value(1), Replaces: "missing"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("replacing unknown = %v", err)
	}
	other := f.register(Kind{Name: "other", Version: 1, Shape: objectShape})
	if _, err := f.s.Record(f.ctx, Request{ID: "e", Kind: other, Subject: f.subject(), Value: value(1), Replaces: "b"}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("replacing across kinds = %v", err)
	}
}

func TestIdenticalRetryIsIdempotentButDifferentContentConflicts(t *testing.T) {
	f := setup(t, Options{})
	kind := f.register(Kind{Name: "note", Version: 1, Shape: objectShape})
	f.record(Request{ID: "a", Kind: kind})
	seq, _ := f.s.Sequence(f.ctx)
	f.record(Request{ID: "a", Kind: kind})
	if now, _ := f.s.Sequence(f.ctx); now != seq {
		t.Fatalf("retry advanced the sequence from %d to %d", seq, now)
	}
	if _, err := f.s.Record(f.ctx, Request{ID: "a", Kind: kind, Subject: f.subject(), Value: value(2)}); !errors.Is(err, ErrConflict) {
		t.Fatalf("same ID different value = %v", err)
	}
	var stage *RecordError
	_, err := f.s.Record(f.ctx, Request{ID: "a", Kind: kind, Subject: f.subject(), Value: value(2)})
	if !errors.As(err, &stage) || stage.Committed {
		t.Fatalf("stage = %+v", stage)
	}
	// A retry stays idempotent after the assertion has been replaced.
	f.record(Request{ID: "b", Kind: kind, Replaces: "a"})
	f.record(Request{ID: "a", Kind: kind})
	if now := ids(f.list(Query{Kind: kind})); now != "b" {
		t.Fatalf("current = %s", now)
	}
	// Registration is idempotent too.
	f.register(Kind{Name: "note", Version: 1, Shape: objectShape})
	if _, err := f.s.Register(f.ctx, Kind{Name: "note", Version: 1, Shape: objects.Shape{Type: "string"}}, Writer{}); !errors.Is(err, ErrConflict) {
		t.Fatalf("re-register with another shape = %v", err)
	}
}

func TestValueValidationAndRefRejections(t *testing.T) {
	f := setup(t, Options{})
	kind := f.register(Kind{Name: "note", Version: 1, Shape: objectShape})
	bad := []Request{
		{Kind: kind, Subject: f.subject(), Value: json.RawMessage(`{"n":"one"}`)},
		{Kind: kind, Subject: f.subject(), Value: json.RawMessage(`{bad`)},
		{Kind: kind, Subject: provenance.Ref{}, Value: value(1)},
		{Kind: kind, Subject: provenance.Ref{Artifact: string(f.artifact), Path: "f", Digest: string(f.file)}, Value: value(1)},
		{Kind: kind, Subject: f.subject(), Value: value(1), Key: "unkeyed-kind"},
		{Kind: kind, Subject: provenance.Ref{Artifact: string(f.artifact), Revision: "r", Row: "row"}, Value: value(1)},
		{Kind: kind, Subject: provenance.Ref{Assertion: "nope"}, Value: value(1)},
		{Kind: kind, Subject: provenance.Ref{Activity: "nope"}, Value: value(1)},
		{Kind: kind, Subject: f.subject(), Value: value(1), Activity: "unvalidatable"},
	}
	for i, r := range bad {
		if _, err := f.s.Record(f.ctx, r); err == nil {
			t.Errorf("request %d recorded: %+v", i, r)
		}
	}
	if _, err := f.s.Record(f.ctx, Request{Kind: "absent@1", Subject: f.subject(), Value: value(1)}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown kind = %v", err)
	}
	if seq, _ := f.s.Sequence(f.ctx); seq != 1 {
		t.Fatalf("rejected writes reached the journal: sequence %d", seq)
	}
	// Every grammar form with a target this layer can reach is accepted.
	first := f.record(Request{Kind: kind})
	for _, subject := range []provenance.Ref{
		{Digest: string(f.file)},
		{Digest: string(f.file), Offset: 1, Length: 2},
		{Artifact: string(f.artifact), Path: "f"},
		{Artifact: string(f.artifact), Path: "f", Offset: 1, Length: 2},
		{Assertion: string(first)},
		{URL: "https://example.test/x", Retrieval: "etag-1"},
	} {
		f.record(Request{Kind: kind, Subject: subject})
		if got := f.list(Query{Subject: ptr(subject)}); len(got) != 1 {
			t.Errorf("subject %+v found %d", subject, len(got))
		}
	}
	for _, subject := range []provenance.Ref{
		{Digest: string(f.file), Offset: 1, Length: 100},
		{Artifact: string(f.artifact), Path: "missing"},
	} {
		if _, err := f.s.Record(f.ctx, Request{Kind: kind, Subject: subject, Value: value(1)}); err == nil {
			t.Errorf("subject %+v accepted", subject)
		}
	}
}

// TestRacingWritersOnOneKey is qualification scenario 3.
func TestRacingWritersOnOneKey(t *testing.T) {
	f := setup(t, Options{})
	kind := f.register(Kind{Name: "owner", Version: 1, Shape: objectShape, Keyed: true})
	const writers = 100
	var wins, conflicts atomic.Int64
	var other atomic.Int64
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := f.s.Record(f.ctx, Request{ID: ID(fmt.Sprintf("w%d", i)), Kind: kind, Subject: f.subject(), Value: value(i), Key: "the-key"})
			switch {
			case err == nil:
				wins.Add(1)
			case errors.Is(err, ErrConflict):
				conflicts.Add(1)
			default:
				other.Add(1)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 || conflicts.Load() != writers-1 || other.Load() != 0 {
		t.Fatalf("wins=%d conflicts=%d other=%d", wins.Load(), conflicts.Load(), other.Load())
	}
	winner, err := f.s.Current(f.ctx, kind, "the-key")
	if err != nil || !winner.Current {
		t.Fatalf("current = %+v err = %v", winner, err)
	}
	// Racing replacements of the winner: again exactly one.
	wins.Store(0)
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := f.s.Record(f.ctx, Request{ID: ID(fmt.Sprintf("r%d", i)), Kind: kind, Subject: f.subject(), Value: value(i), Key: "the-key", Replaces: winner.ID})
			if err == nil {
				wins.Add(1)
			} else if !errors.Is(err, ErrConflict) {
				other.Add(1)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 || other.Load() != 0 {
		t.Fatalf("replacement wins=%d other=%d", wins.Load(), other.Load())
	}
	next, err := f.s.Current(f.ctx, kind, "the-key")
	if err != nil || next.Replaces != winner.ID {
		t.Fatalf("next = %+v err = %v", next, err)
	}
	f.reopen()
	reopened, err := f.s.Current(f.ctx, kind, "the-key")
	if err != nil || reopened.ID != next.ID {
		t.Fatalf("after reopen = %+v err = %v", reopened, err)
	}
	if chain, err := f.s.History(f.ctx, winner.ID); err != nil || len(chain) != 2 {
		t.Fatalf("history = %s err = %v", ids(chain), err)
	}
	// After retraction the key is free again, and only then.
	if err := f.s.Retract(f.ctx, Retraction{Assertion: next.ID}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.s.Current(f.ctx, kind, "the-key"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("current after retract = %v", err)
	}
	f.record(Request{ID: "fresh", Kind: kind, Key: "the-key"})
	if a, _ := f.s.Current(f.ctx, kind, "the-key"); a.ID != "fresh" {
		t.Fatalf("current = %+v", a)
	}
}

func TestKeyedKindRequiresKey(t *testing.T) {
	f := setup(t, Options{})
	kind := f.register(Kind{Name: "owner", Version: 1, Shape: objectShape, Keyed: true})
	if _, err := f.s.Record(f.ctx, Request{Kind: kind, Subject: f.subject(), Value: value(1)}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("keyless = %v", err)
	}
	if _, err := f.s.List(f.ctx, Query{Key: "k"}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("key without kind = %v", err)
	}
	f.record(Request{ID: "a", Kind: kind, Key: "k"})
	f.record(Request{ID: "b", Kind: kind, Key: "other"})
	if got := ids(f.list(Query{Kind: kind, Key: "k"})); got != "a" {
		t.Fatalf("by key = %s", got)
	}
}

func TestEvidenceAsOfAndPaging(t *testing.T) {
	f := setup(t, Options{})
	kind := f.register(Kind{Name: "note", Version: 1, Shape: objectShape})
	cited := provenance.Ref{Digest: string(f.file)}
	a := f.record(Request{ID: "a", Kind: kind, Evidence: []provenance.Ref{cited, cited}})
	seqA, _ := f.s.Sequence(f.ctx)
	f.record(Request{ID: "b", Kind: kind, Evidence: []provenance.Ref{cited}, Replaces: a})
	seqB, _ := f.s.Sequence(f.ctx)
	if got := ids(f.list(Query{Evidence: &cited})); got != "b" {
		t.Fatalf("citing current = %s", got)
	}
	if got := ids(f.list(Query{Evidence: &cited, IncludeEnded: true})); got != "a,b" {
		t.Fatalf("citing all = %s", got)
	}
	if got := f.list(Query{Kind: kind, AsOf: seqA}); ids(got) != "a" || !got[0].Current {
		t.Fatalf("as of %d = %+v", seqA, got)
	}
	if got := ids(f.list(Query{Kind: kind, AsOf: seqB})); got != "b" {
		t.Fatalf("as of %d = %s", seqB, got)
	}
	for i := range 25 {
		f.record(Request{ID: ID(fmt.Sprintf("p%02d", i)), Kind: kind})
	}
	seen, cursor := 0, ""
	for {
		page, err := f.s.List(f.ctx, Query{Kind: kind, Limit: 10, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		// Later writes never enter a snapshot's remaining pages.
		f.record(Request{ID: ID(fmt.Sprintf("late%d", seen)), Kind: kind})
		seen += len(page.Assertions)
		if cursor = page.NextCursor; cursor == "" {
			break
		}
	}
	if seen != 26 {
		t.Fatalf("paged %d, want 26", seen)
	}
	if _, err := f.s.List(f.ctx, Query{Kind: kind, Cursor: "garbage"}); !errors.Is(err, ErrCursor) {
		t.Fatalf("bad cursor = %v", err)
	}
}

// TestOccurrencesDoNotBleed is qualification scenario 2.
func TestOccurrencesDoNotBleed(t *testing.T) {
	f := setup(t, Options{})
	kind := f.register(Kind{Name: "note", Version: 1, Shape: objectShape})
	one, err := f.store.RecordOccurrence(f.ctx, artifacts.OccurrenceRequest{ID: "one", Artifact: f.artifact, Name: "first.bin"})
	if err != nil {
		t.Fatal(err)
	}
	two, err := f.store.RecordOccurrence(f.ctx, artifacts.OccurrenceRequest{ID: "two", Artifact: f.artifact, Name: "second.bin"})
	if err != nil {
		t.Fatal(err)
	}
	onOne := provenance.Ref{Occurrence: string(one.ID)}
	onTwo := provenance.Ref{Occurrence: string(two.ID)}
	onContent := provenance.Ref{Digest: string(one.Content)}
	f.record(Request{ID: "about-one", Kind: kind, Subject: onOne})
	f.record(Request{ID: "about-content", Kind: kind, Subject: onContent})
	if got := ids(f.list(Query{Subject: &onOne})); got != "about-one" {
		t.Fatalf("one = %s", got)
	}
	if got := f.list(Query{Subject: &onTwo}); len(got) != 0 {
		t.Fatalf("assertion bled onto the other occurrence: %s", ids(got))
	}
	if got := ids(f.list(Query{Subject: &onTwo, IncludeContent: true})); got != "about-content" {
		t.Fatalf("two with content = %s", got)
	}
	if got := ids(f.list(Query{Subject: &onOne, IncludeContent: true})); got != "about-one,about-content" {
		t.Fatalf("one with content = %s", got)
	}
	page, err := f.store.Occurrences(f.ctx, artifacts.OccurrenceQuery{Content: one.Content})
	if err != nil || len(page.Occurrences) != 2 {
		t.Fatalf("shared content query = %+v err = %v", page, err)
	}
}

func TestInternalCategoryIsHiddenByDefault(t *testing.T) {
	f := setup(t, Options{})
	kind := f.register(Kind{Name: "note", Version: 1, Shape: objectShape})
	id, err := f.s.Record(observability.WithInternal(f.ctx), Request{Kind: kind, Subject: f.subject(), Value: value(1)})
	if err != nil {
		t.Fatal(err)
	}
	if got := f.list(Query{Kind: kind}); len(got) != 0 {
		t.Fatalf("internal listed by default: %s", ids(got))
	}
	if got := f.list(Query{Kind: kind, IncludeInternal: true}); ids(got) != string(id) || got[0].Category != observability.Internal {
		t.Fatalf("include internal = %+v", got)
	}
	page, err := f.s.Changes(f.ctx, provenance.ChangeQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Changes) != 1 || page.Changes[0].Class != ChangeKind || page.Last != 2 {
		t.Fatalf("changes = %+v", page)
	}
}

func TestChangeFeedResumesInOrder(t *testing.T) {
	f := setup(t, Options{})
	kind := f.register(Kind{Name: "note", Version: 1, Shape: objectShape})
	woken := make(chan uint64, 64)
	stop := f.s.OnCommit(func(sequence uint64) {
		select {
		case woken <- sequence:
		default:
		}
	})
	f.record(Request{ID: "a", Kind: kind, Actor: "alice", Value: value(1)})
	f.record(Request{ID: "b", Kind: kind, Replaces: "a"})
	if err := f.s.Retract(f.ctx, Retraction{Assertion: "b", Actor: "bob"}); err != nil {
		t.Fatal(err)
	}
	<-woken
	stop()
	page, err := f.s.Changes(f.ctx, provenance.ChangeQuery{After: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Changes) != 3 || page.Last != 4 {
		t.Fatalf("page = %+v", page)
	}
	want := []provenance.Change{
		{Sequence: 2, Class: ChangeAssertion, ID: "a", Kind: string(kind), Actor: "alice"},
		{Sequence: 3, Class: ChangeAssertion, ID: "b", Kind: string(kind)},
		{Sequence: 4, Class: ChangeRetraction, ID: "b", Actor: "bob"},
	}
	for i := range want {
		if page.Changes[i] != want[i] {
			t.Errorf("change %d = %+v, want %+v", i, page.Changes[i], want[i])
		}
	}
}

// TestForgery is qualification scenario 4 and A6's acceptance.
func TestForgery(t *testing.T) {
	authority := NewAuthority()
	f := setup(t, Options{Authority: authority, ReservedNamespaces: map[string][]string{"secure.": {"registrar"}}})
	registrar := authority.Credential("registrar")
	issuer := authority.Credential("issuer")
	kind, err := f.s.Register(f.ctx, Kind{Name: "secure.cert", Version: 1, Shape: objectShape, Writers: []string{"issuer"}, Keyed: true}, registrar)
	if err != nil {
		t.Fatal(err)
	}
	attempts := map[string]Writer{
		"zero value":       {},
		"wrong identity":   authority.Credential("evaluator"),
		"empty identity":   authority.Credential(""),
		"other authority":  NewAuthority().Credential("issuer"),
		"no authority":     (*Authority)(nil).Credential("issuer"),
		"registrar is not": registrar,
	}
	for name, writer := range attempts {
		_, err := f.s.Record(f.ctx, Request{ID: "forged", Kind: kind, Subject: f.subject(), Value: value(1), Key: "k", Writer: writer})
		if !errors.Is(err, ErrWriterForbidden) {
			t.Errorf("%s = %v, want ErrWriterForbidden", name, err)
		}
	}
	if seq, _ := f.s.Sequence(f.ctx); seq != 1 {
		t.Fatalf("forged writes reached the journal: sequence %d", seq)
	}

	real, err := f.s.Record(f.ctx, Request{ID: "real", Kind: kind, Subject: f.subject(), Value: value(1), Key: "k", Writer: issuer, Actor: "claimed-by-caller"})
	if err != nil {
		t.Fatal(err)
	}
	stored, err := f.s.Get(f.ctx, real)
	if err != nil || stored.Writer != "issuer" || stored.Actor != "claimed-by-caller" {
		t.Fatalf("stored = %+v err = %v", stored, err)
	}

	for name, writer := range attempts {
		_, err := f.s.Record(f.ctx, Request{ID: "forged", Kind: kind, Subject: f.subject(), Value: value(2), Key: "k", Writer: writer, Replaces: real})
		if !errors.Is(err, ErrWriterForbidden) {
			t.Errorf("replace by %s = %v", name, err)
		}
		if err := f.s.Retract(f.ctx, Retraction{Assertion: real, Writer: writer}); !errors.Is(err, ErrWriterForbidden) {
			t.Errorf("retract by %s = %v", name, err)
		}
	}

	// Squatting: without the registrar credential the namespace, including new
	// versions of an existing kind, is closed.
	for name, writer := range map[string]Writer{"zero": {}, "wrong": issuer, "foreign": NewAuthority().Credential("registrar")} {
		if _, err := f.s.Register(f.ctx, Kind{Name: "secure.cert", Version: 2, Shape: objectShape}, writer); err == nil {
			t.Errorf("%s registered a new version of a reserved kind", name)
		}
		if _, err := f.s.Register(f.ctx, Kind{Name: "secure.other", Version: 1, Shape: objectShape}, writer); !errors.Is(err, ErrNamespace) && !errors.Is(err, ErrWriterForbidden) {
			t.Errorf("%s registered in a reserved namespace: %v", name, err)
		}
	}

	// A kind cannot be widened by re-registering it with other writers.
	if _, err := f.s.Register(f.ctx, Kind{Name: "secure.cert", Version: 1, Shape: objectShape, Writers: []string{"issuer", "attacker"}, Keyed: true}, registrar); !errors.Is(err, ErrConflict) {
		t.Fatalf("widening writers = %v", err)
	}

	// Writes to other layers mimicking the encoding change nothing here.
	encoding, err := json.Marshal(Assertion{ID: "forged", Kind: kind, Subject: f.subject(), Value: value(9), Key: "k", Writer: "issuer"})
	if err != nil {
		t.Fatal(err)
	}
	put, err := f.cas.Put(f.ctx, strings.NewReader(string(encoding)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.Publish(f.ctx, artifacts.Manifest{Version: 1, Entries: []artifacts.Entry{{Path: "a/forged", Content: put.ID, Size: put.Size}}, Metadata: map[string]json.RawMessage{"writer": json.RawMessage(`"issuer"`), "kind": json.RawMessage(`"` + string(kind) + `"`)}}); err != nil {
		t.Fatal(err)
	}
	line, err := lineage.New(f.store, lineage.Options{Root: filepath.Join(f.root, "lineage"), Relaxed: true})
	if err != nil {
		t.Fatal(err)
	}
	defer line.Close()
	if _, err := line.Record(f.ctx, lineage.Record{ID: "a/forged", Activity: &lineage.Activity{Kind: string(kind), Actor: "issuer"}}); err != nil {
		t.Fatal(err)
	}
	f.reopen()
	if _, err := f.s.Get(f.ctx, "forged"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("forged assertion visible: %v", err)
	}
	current, err := f.s.Current(f.ctx, kind, "k")
	if err != nil || current.ID != real {
		t.Fatalf("current = %+v err = %v", current, err)
	}

	// The default handle on a service without an authority cannot host a restricted kind.
	bare := setup(t, Options{})
	if _, err := bare.s.Register(bare.ctx, Kind{Name: "x", Version: 1, Shape: objectShape, Writers: []string{"issuer"}}, Writer{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("restricted kind without authority = %v", err)
	}
}

func TestRestrictedKindAfterReopenWithFreshAuthorityInstance(t *testing.T) {
	authority := NewAuthority()
	f := setup(t, Options{Authority: authority})
	writer := authority.Credential("issuer")
	kind, err := f.s.Register(f.ctx, Kind{Name: "cert", Version: 1, Shape: objectShape, Writers: []string{"issuer"}}, Writer{})
	if err != nil {
		t.Fatal(err)
	}
	// Identities are persisted, the capability is not: a new process builds a new
	// authority and old credentials stop working.
	next := NewAuthority()
	f.options.Authority = next
	f.reopen()
	if _, err := f.s.Record(f.ctx, Request{Kind: kind, Subject: f.subject(), Value: value(1), Writer: writer}); !errors.Is(err, ErrWriterForbidden) {
		t.Fatalf("old credential = %v", err)
	}
	if _, err := f.s.Record(f.ctx, Request{Kind: kind, Subject: f.subject(), Value: value(1), Writer: next.Credential("issuer")}); err != nil {
		t.Fatal(err)
	}
}

func TestLeaseExcludesSecondLiveOwner(t *testing.T) {
	f := setup(t, Options{})
	if _, err := New(f.store, Options{Root: filepath.Join(f.root, "assertions")}); !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("second owner = %v", err)
	}
	var lease *LeaseError
	_, err := New(f.store, Options{Root: filepath.Join(f.root, "assertions")})
	if !errors.As(err, &lease) || lease.Owner.PID == 0 {
		t.Fatalf("lease = %+v err = %v", lease, err)
	}
}

func TestClosedServiceRefusesWork(t *testing.T) {
	f := setup(t, Options{})
	kind := f.register(Kind{Name: "note", Version: 1, Shape: objectShape})
	if err := f.s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.s.Record(f.ctx, Request{Kind: kind, Subject: f.subject(), Value: value(1)}); !errors.Is(err, ErrClosed) {
		t.Fatalf("record = %v", err)
	}
	if _, err := f.s.List(f.ctx, Query{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("list = %v", err)
	}
	if _, err := f.s.Sequence(f.ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("sequence = %v", err)
	}
}

func TestIndexedLookupDoesNotDependOnJournalSize(t *testing.T) {
	f := setup(t, Options{})
	kind := f.register(Kind{Name: "note", Version: 1, Shape: objects.Shape{Type: "integer"}, Keyed: true})
	for i := range 3000 {
		if _, err := f.s.Record(f.ctx, Request{ID: ID(fmt.Sprintf("a%d", i)), Kind: kind, Subject: f.subject(), Value: json.RawMessage(fmt.Sprint(i)), Key: fmt.Sprint(i)}); err != nil {
			t.Fatal(err)
		}
	}
	// A posting list carries exactly the matching records, however long the log is.
	if got := len(f.s.index.byKey[keyPosting(kind, "1234")]); got != 1 {
		t.Fatalf("posting length = %d", got)
	}
	f.reopen()
	if got := len(f.s.index.byKey[keyPosting(kind, "1234")]); got != 1 {
		t.Fatalf("posting length after rebuild = %d", got)
	}
	a, err := f.s.Current(f.ctx, kind, "1234")
	if err != nil || a.ID != "a1234" {
		t.Fatalf("current = %+v err = %v", a, err)
	}
}
