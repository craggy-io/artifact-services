package lineage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/craggy-io/artifact-services/artifacts"
	"github.com/craggy-io/artifact-services/cas"
	"github.com/craggy-io/artifact-services/observability"
	"github.com/craggy-io/artifact-services/provenance"
	"github.com/craggy-io/artifact-services/storage"
)

type fixture struct {
	t         *testing.T
	cas       *cas.Service
	artifacts *artifacts.Service
	service   *Service
	root      string
}

func setup(t *testing.T) *fixture {
	t.Helper()
	root := t.TempDir()
	disk, err := storage.NewFilesystem(filepath.Join(root, "blobs"))
	if err != nil {
		t.Fatal(err)
	}
	content, err := cas.New(disk, cas.Options{TempDir: root})
	if err != nil {
		t.Fatal(err)
	}
	art, err := artifacts.New(content, artifacts.Options{Root: filepath.Join(root, "catalog")})
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{t: t, cas: content, artifacts: art, root: filepath.Join(root, "lineage")}
	f.service, err = New(art, Options{Root: f.root, ValidateSelector: func(_ context.Context, r provenance.Ref) error {
		if r.Row != "" && !strings.HasPrefix(r.Row, "row-") {
			return ErrInvalid
		}
		if r.Column != "" && r.Column != "column-1" {
			return ErrInvalid
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.service.Close(); art.Close() })
	return f
}
func (f *fixture) artifact(ctx context.Context, path, data string) artifacts.ID {
	f.t.Helper()
	blob, err := f.cas.Put(ctx, strings.NewReader(data))
	if err != nil {
		f.t.Fatal(err)
	}
	id, err := f.artifacts.Publish(ctx, artifacts.Manifest{Entries: []artifacts.Entry{{Path: path, Content: blob.ID, Size: blob.Size}}})
	if err != nil {
		f.t.Fatal(err)
	}
	return id
}
func (f *fixture) record(r Record) {
	f.t.Helper()
	id, err := f.service.Record(context.Background(), r)
	if err != nil || id != r.ID {
		f.t.Fatalf("Record %s: %s %v", r.ID, id, err)
	}
}
func (f *fixture) revision(id ID, artifact artifacts.ID, parents ...ID) {
	f.t.Helper()
	f.record(Record{ID: id, Revision: &Revision{Artifact: artifact, Parents: parents}})
}

func TestPersistenceIdentityAndIndependentHistories(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	a := f.artifact(ctx, "data.txt", "same bytes")
	f.revision("A1", a)
	f.revision("B1", a)
	f.revision("A2", a, "A1")
	input := Record{ID: "import-A", Activity: &Activity{Kind: "import", Outputs: []provenance.Ref{{Artifact: string(a), Revision: "A1"}}}}
	f.record(input)
	f.record(input)
	input.Activity.Kind = "different"
	if _, err := f.service.Record(ctx, input); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflict: %v", err)
	}
	if _, err := f.service.Record(ctx, Record{ID: "bad", Revision: &Revision{Artifact: a, Parents: []ID{"missing"}}}); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := f.service.Record(ctx, Record{ID: "self", Revision: &Revision{Artifact: a, Parents: []ID{"self"}}}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	page, err := f.service.List(ctx, Query{Kind: "revision", Ref: &provenance.Ref{Revision: "A1"}, Role: "parent"})
	if err != nil || len(page.Records) != 1 || page.Records[0].ID != "A2" {
		t.Fatal(page, err)
	}
	traversal, err := f.service.Traverse(ctx, TraversalQuery{Root: provenance.Ref{Revision: "A2"}, Kinds: []string{"revision-of"}})
	if err != nil || len(traversal.Edges) != 1 || traversal.Edges[0].To.Revision != "A1" {
		t.Fatal(traversal, err)
	}
	if err := f.service.Close(); err != nil {
		t.Fatal(err)
	}
	f.service, err = New(f.artifacts, Options{Root: f.root})
	if err != nil {
		t.Fatal(err)
	}
	got, err := f.service.Get(ctx, "B1")
	if err != nil || got.Revision.Artifact != a {
		t.Fatal(got, err)
	}
	got.Revision.Parents = append(got.Revision.Parents, "mutated")
	again, _ := f.service.Get(ctx, "B1")
	if len(again.Revision.Parents) != 0 {
		t.Fatal("Get aliases state")
	}
}

func TestSnapshotPagingFilteringAndConcurrentRecords(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	a := f.artifact(ctx, "data.txt", "data")
	var wg sync.WaitGroup
	for i := range 12 {
		wg.Go(func() {
			call := ctx
			if i%2 == 0 {
				call = observability.WithInternal(call)
			}
			_, err := f.service.Record(call, Record{ID: ID(fmt.Sprintf("event-%02d", i)), Activity: &Activity{Kind: "annotation", Inputs: []provenance.Ref{{Artifact: string(a)}}}})
			if err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	first, err := f.service.List(ctx, Query{Limit: 2})
	if err != nil || len(first.Records) != 2 || first.NextCursor == "" {
		t.Fatal(first, err)
	}
	f.record(Record{ID: "event-99", Activity: &Activity{Kind: "annotation"}})
	count := len(first.Records)
	cursor := first.NextCursor
	for cursor != "" {
		page, err := f.service.List(ctx, Query{Limit: 2, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range page.Records {
			if r.ID == "event-99" {
				t.Fatal("new commit entered cursor")
			}
		}
		count += len(page.Records)
		cursor = page.NextCursor
	}
	if count != 6 {
		t.Fatal(count)
	}
	if _, err := f.service.List(ctx, Query{Limit: 3, Cursor: first.NextCursor}); err == nil {
		t.Fatal("cursor query changed")
	}
	all, err := f.service.List(ctx, Query{IncludeInternal: true})
	if err != nil || len(all.Records) != 13 {
		t.Fatal(all, err)
	}
	if _, err := f.service.Record(ctx, Record{ID: "event-00", Activity: &Activity{Kind: "annotation", Inputs: []provenance.Ref{{Artifact: string(a)}}}}); !errors.Is(err, ErrConflict) {
		t.Fatal("classification changed", err)
	}
}

func TestMappingJoinGroupColumnAndPredicates(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	a := f.artifact(ctx, "a.ndjson", "a")
	b := f.artifact(ctx, "b.ndjson", "b")
	c := f.artifact(ctx, "c.ndjson", "c")
	schema := f.artifact(ctx, "schema.json", "{}")
	f.revision("A1", a)
	f.revision("B1", b)
	f.revision("C1", c)
	ref := func(artifact artifacts.ID, revision, row, column string) provenance.Ref {
		return provenance.Ref{Artifact: string(artifact), Revision: revision, Schema: string(schema), Row: row, Column: column}
	}
	output := ref(c, "C1", "row-c", "")
	left := ref(a, "A1", "row-a", "")
	right := ref(b, "B1", "row-b", "")
	mappings := []provenance.Mapping{{Output: output, Inputs: []provenance.Dependency{{Ref: left, Role: provenance.Value}, {Ref: right, Role: provenance.Value}, {Ref: left, Role: provenance.Predicate}}}, {Output: ref(c, "C1", "", "column-1"), Inputs: []provenance.Dependency{{Ref: ref(a, "A1", "", "column-1"), Role: provenance.Value}, {Ref: ref(b, "B1", "", "column-1"), Role: provenance.Value}}}}
	var data bytes.Buffer
	enc := json.NewEncoder(&data)
	for _, mapping := range mappings {
		if err := enc.Encode(mapping); err != nil {
			t.Fatal(err)
		}
	}
	evidenceID := f.artifact(observability.WithInternal(ctx), "mappings.ndjson", data.String())
	coverage := provenance.Coverage{Rows: provenance.Complete, Columns: provenance.Complete, Values: provenance.Complete, Predicates: provenance.Complete}
	f.record(Record{ID: "join", Activity: &Activity{Kind: "query", Inputs: []provenance.Ref{{Revision: "A1"}, {Revision: "B1"}}, Outputs: []provenance.Ref{{Revision: "C1"}}, Evidence: []Evidence{{Artifact: evidenceID, Path: "mappings.ndjson", Coverage: coverage}}}})
	page, err := f.service.Traverse(ctx, TraversalQuery{Root: output, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	edges := page.Edges
	for page.NextCursor != "" {
		page, err = f.service.Traverse(ctx, TraversalQuery{Root: output, Limit: 1, Cursor: page.NextCursor})
		if err != nil {
			t.Fatal(err)
		}
		edges = append(edges, page.Edges...)
	}
	if len(edges) != 3 {
		t.Fatalf("join/group dependencies: %+v", edges)
	}
	predicate, err := f.service.Traverse(ctx, TraversalQuery{Root: output, DependencyRole: provenance.Predicate})
	if err != nil || len(predicate.Edges) != 1 {
		t.Fatal(predicate, err)
	}
	columns, err := f.service.Traverse(ctx, TraversalQuery{Root: ref(c, "C1", "", "column-1")})
	if err != nil || len(columns.Edges) != 2 {
		t.Fatal(columns, err)
	}
	consumers, err := f.service.List(ctx, Query{Ref: &left, Role: "input", DependencyRole: provenance.Value})
	if err != nil || len(consumers.Records) != 1 {
		t.Fatal(consumers, err)
	}
	bad := left
	bad.Revision = "B1"
	if _, err := f.service.Record(ctx, Record{ID: "mismatch", Activity: &Activity{Kind: "x", Inputs: []provenance.Ref{bad}}}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := f.service.Record(ctx, Record{ID: "badrow", Activity: &Activity{Kind: "x", Inputs: []provenance.Ref{ref(a, "A1", "offset-1", "")}}}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}

func TestTraversalBoundsCycleAndCancellation(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	a := f.artifact(ctx, "a", "a")
	b := f.artifact(ctx, "b", "b")
	c := f.artifact(ctx, "c", "c")
	refs := []provenance.Ref{{Artifact: string(a)}, {Artifact: string(b)}, {Artifact: string(c)}}
	for i := range refs {
		f.record(Record{ID: ID(fmt.Sprintf("edge-%d", i)), Relationship: &Relationship{Kind: "derived-from", From: refs[i], To: refs[(i+1)%3]}})
	}
	full, err := f.service.Traverse(ctx, TraversalQuery{Root: refs[0]})
	if err != nil || len(full.Edges) != 3 || len(full.Nodes) != 3 {
		t.Fatal(full, err)
	}
	bound, err := f.service.Traverse(ctx, TraversalQuery{Root: refs[0], MaxNodes: 2})
	if err != nil || !bound.Truncated || bound.Reason != "nodes" {
		t.Fatal(bound, err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := f.service.List(canceled, Query{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := f.service.Traverse(canceled, TraversalQuery{Root: refs[0]}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := f.service.Record(canceled, Record{ID: "cancel", Activity: &Activity{Kind: "x"}}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestInternalArtifactVisibilityAndRegisteredReferences(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	user := f.artifact(ctx, "user", "user")
	support := f.artifact(observability.WithInternal(ctx), "support", "support")
	f.record(Record{ID: "support-edge", Relationship: &Relationship{Kind: "derived-from", From: provenance.Ref{Artifact: string(user)}, To: provenance.Ref{Artifact: string(support)}}})
	visible, err := f.service.Traverse(ctx, TraversalQuery{Root: provenance.Ref{Artifact: string(user)}})
	if err != nil || len(visible.Edges) != 0 || len(visible.Nodes) != 1 {
		t.Fatal(visible, err)
	}
	diagnostic, err := f.service.Traverse(ctx, TraversalQuery{Root: provenance.Ref{Artifact: string(user)}, IncludeInternal: true})
	if err != nil || len(diagnostic.Edges) != 1 || len(diagnostic.Nodes) != 2 {
		t.Fatal(diagnostic, err)
	}
	// Valid manifest bytes in CAS are not necessarily accepted artifact membership.
	blob, err := f.cas.Put(ctx, strings.NewReader("unregistered data"))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(artifacts.Manifest{Version: 1, Entries: []artifacts.Entry{{Path: "unregistered", Content: blob.ID, Size: blob.Size}}})
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := f.cas.Put(ctx, bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.Record(ctx, Record{ID: "unregistered", Activity: &Activity{Kind: "import", Outputs: []provenance.Ref{{Artifact: string(manifest.ID)}}}}); !errors.Is(err, artifacts.ErrNotFound) {
		t.Fatal(err)
	}
}

func TestSelectorCapabilityAndCursorAfterClose(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	a := f.artifact(ctx, "data", "data")
	f.revision("A1", a)
	other, err := New(f.artifacts, Options{Root: filepath.Join(t.TempDir(), "other")})
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if _, err := other.Record(ctx, Record{ID: "A1", Revision: &Revision{Artifact: a}}); err != nil {
		t.Fatal(err)
	}
	if _, err := other.Record(ctx, Record{ID: "detailed", Activity: &Activity{Kind: "x", Inputs: []provenance.Ref{{Artifact: string(a), Revision: "A1", Row: "row-1"}}}}); !errors.Is(err, ErrUnsupportedSelector) {
		t.Fatal(err)
	}
	f.record(Record{ID: "z", Activity: &Activity{Kind: "x"}})
	first, err := f.service.List(ctx, Query{Limit: 1})
	if err != nil || first.NextCursor == "" {
		t.Fatal(first, err)
	}
	if err := f.service.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.List(ctx, Query{Limit: 1, Cursor: first.NextCursor}); err == nil {
		t.Fatal("closed service replayed cursor")
	}
}
