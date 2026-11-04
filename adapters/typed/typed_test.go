package typed_test

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/craggy-io/artifact-services/adapters/testkit"
	"github.com/craggy-io/artifact-services/adapters/typed"
	"github.com/craggy-io/artifact-services/assertions"
	"github.com/craggy-io/artifact-services/lineage"
	"github.com/craggy-io/artifact-services/objects"
	"github.com/craggy-io/artifact-services/provenance"
)

type finding struct {
	Title    string   `json:"title"`
	Severity int      `json:"severity"`
	Tags     []string `json:"tags"`
	Note     *string  `json:"note"`
	Extra    string   `json:"extra,omitempty"`
}

func collect[T any](t *testing.T, seq func(func(typed.Said[T], error) bool)) []typed.Said[T] {
	t.Helper()
	var out []typed.Said[T]
	for said, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, said)
	}
	return out
}

func TestShapeOf(t *testing.T) {
	shape, err := typed.ShapeOf[finding]()
	if err != nil {
		t.Fatal(err)
	}
	if shape.Type != "object" || shape.Properties["severity"].Type != "integer" || shape.Properties["tags"].Items.Type != "string" || !shape.Properties["note"].Nullable {
		t.Fatalf("shape = %+v", shape)
	}
	if !reflect.DeepEqual(shape.Required, []string{"title", "severity", "tags", "note"}) {
		t.Fatalf("required = %v", shape.Required)
	}
	if _, err := typed.ShapeOf[map[string]int](); !errors.Is(err, typed.ErrUnsupportedType) {
		t.Fatalf("map = %v", err)
	}
	type recursive struct {
		Next *recursive `json:"next"`
	}
	if _, err := typed.ShapeOf[recursive](); !errors.Is(err, typed.ErrUnsupportedType) {
		t.Fatalf("recursive = %v", err)
	}
}

func TestKindRoundTripAndQueries(t *testing.T) {
	s := testkit.Open(t)
	kind, err := typed.DefineKind[finding](s.Ctx, s.Assertions, typed.KindDefinition{Name: "finding", Version: 1, Keyed: true})
	if err != nil {
		t.Fatal(err)
	}
	artifact := s.File("f", "x")
	subject := provenance.Ref{Artifact: string(artifact)}
	cited := provenance.Ref{URL: "https://example.test", Retrieval: "r"}
	first := finding{Title: "one", Severity: 1, Tags: []string{"a"}}
	id, err := kind.Claim(s.Ctx, subject, first, typed.WithKey("k"), typed.WithID("c1"), typed.WithEvidence(cited), typed.WithActor("alice"))
	if err != nil {
		t.Fatal(err)
	}
	said, err := kind.Keyed(s.Ctx, "k")
	if err != nil || said.ID != id || said.Actor != "alice" || !reflect.DeepEqual(said.Claim, first) {
		t.Fatalf("keyed = %+v err = %v", said, err)
	}
	if got := collect(t, kind.About(s.Ctx, subject)); len(got) != 1 || got[0].Claim.Title != "one" {
		t.Fatalf("about = %+v", got)
	}
	if got := collect(t, kind.Citing(s.Ctx, cited)); len(got) != 1 {
		t.Fatalf("citing = %+v", got)
	}
	next := finding{Title: "two", Severity: 2, Tags: []string{}}
	if _, err := kind.Claim(s.Ctx, subject, next, typed.WithKey("k"), typed.WithID("c2"), typed.Replacing("c1")); err != nil {
		t.Fatal(err)
	}
	if got := collect(t, kind.Current(s.Ctx)); len(got) != 1 || got[0].Claim.Title != "two" {
		t.Fatalf("current = %+v", got)
	}
	old, err := kind.Get(s.Ctx, "c1")
	if err != nil || old.Current || old.Claim.Title != "one" {
		t.Fatalf("old = %+v err = %v", old, err)
	}
	// Values are still validated by the core schema.
	wide, err := typed.DefineKind[map[string]any](s.Ctx, s.Assertions, typed.KindDefinition{Name: "finding", Version: 2, Shape: mustShape[finding](t)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wide.Claim(s.Ctx, subject, map[string]any{"title": 5}); !errors.Is(err, assertions.ErrInvalid) {
		t.Fatalf("bad value = %v", err)
	}
	other, err := typed.DefineKind[int](s.Ctx, s.Assertions, typed.KindDefinition{Name: "other", Version: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.Get(s.Ctx, "c1"); !errors.Is(err, assertions.ErrNotFound) {
		t.Fatalf("get across kinds = %v", err)
	}
}

func mustShape[T any](t *testing.T) *objects.Shape {
	t.Helper()
	shape, err := typed.ShapeOf[T]()
	if err != nil {
		t.Fatal(err)
	}
	return &shape
}

func TestWriterOptionReachesTheAuthorityCheck(t *testing.T) {
	authority := assertions.NewAuthority()
	s := testkit.Open(t, testkit.Options{Authority: authority, Relaxed: true})
	kind, err := typed.DefineKind[int](s.Ctx, s.Assertions, typed.KindDefinition{Name: "gated", Version: 1, Writers: []string{"w"}})
	if err != nil {
		t.Fatal(err)
	}
	subject := provenance.Ref{Artifact: string(s.File("f", "x"))}
	if _, err := kind.Claim(s.Ctx, subject, 1); !errors.Is(err, assertions.ErrWriterForbidden) {
		t.Fatalf("no writer = %v", err)
	}
	id, err := kind.Claim(s.Ctx, subject, 1, typed.WithWriter(authority.Credential("w")), typed.WithActivity(""))
	if err != nil {
		t.Fatal(err)
	}
	if said, err := kind.Get(s.Ctx, id); err != nil || said.Writer != "w" {
		t.Fatalf("said = %+v err = %v", said, err)
	}
}

type record struct {
	ID    string `json:"id_col"`
	Count int    `json:"count"`
}

func services(s *testkit.Stack) typed.Services {
	return typed.Services{CAS: s.CAS, Artifacts: s.Artifacts, Objects: s.Objects}
}

func TestTableRoundTrip(t *testing.T) {
	s := testkit.Open(t)
	table, err := typed.DefineTable[record](s.Ctx, services(s), "id_col")
	if err != nil {
		t.Fatal(err)
	}
	rows := []record{{"a", 1}, {"b", 2}, {"c", 3}}
	id, err := table.Put(s.Ctx, typed.Slice(rows), nil)
	if err != nil {
		t.Fatal(err)
	}
	var got []record
	for row, err := range typed.Rows[record](s.Ctx, s.Objects, id) {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, row)
	}
	if !reflect.DeepEqual(got, rows) {
		t.Fatalf("rows = %+v", got)
	}
	// A row is addressable by its natural key, in lineage and assertions alike.
	rowID, err := table.RowID("b")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Lineage.Append(s.Ctx, lineage.AppendRevision{ID: "t1", History: "table", Artifact: id}); err != nil {
		t.Fatal(err)
	}
	ref := provenance.Ref{Artifact: string(id), Revision: "t1", Row: rowID}
	kind, err := typed.DefineKind[int](s.Ctx, s.Assertions, typed.KindDefinition{Name: "row-note", Version: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kind.Claim(s.Ctx, ref, 1); err != nil {
		t.Fatalf("assert about a keyed row: %v", err)
	}
	if _, err := s.Lineage.Record(s.Ctx, lineage.Record{ID: "uses-row", Relationship: &lineage.Relationship{Kind: "uses", From: ref, To: provenance.Ref{URL: "u", Retrieval: "r"}}}); err != nil {
		t.Fatalf("lineage about a keyed row: %v", err)
	}
	missing, _ := table.RowID("nope")
	ref.Row = missing
	if _, err := kind.Claim(s.Ctx, ref, 1); err == nil {
		t.Fatal("an unknown key was accepted")
	}
	// Early exit from the iterator releases its stream.
	for range typed.Rows[record](s.Ctx, s.Objects, id) {
		break
	}
}

func TestUnkeyedTableAndDuplicateKeys(t *testing.T) {
	s := testkit.Open(t)
	unkeyed, err := typed.DefineTable[record](s.Ctx, services(s))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := unkeyed.Put(s.Ctx, typed.Slice([]record{{"a", 1}, {"a", 1}}), nil); err != nil {
		t.Fatal(err)
	}
	keyed, err := typed.DefineTable[record](s.Ctx, services(s), "id_col")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := keyed.Put(s.Ctx, typed.Slice([]record{{"a", 1}, {"a", 2}}), nil); err == nil {
		t.Fatal("duplicate key accepted")
	}
	failing := func(yield func(record, error) bool) {
		if !yield(record{"a", 1}, nil) {
			return
		}
		yield(record{}, fmt.Errorf("source failed"))
	}
	if _, err := keyed.Put(s.Ctx, failing, nil); err == nil {
		t.Fatal("source error swallowed")
	}
}
