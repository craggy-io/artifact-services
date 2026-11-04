package lineage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/craggy-io/artifact-services/artifacts"
	"github.com/craggy-io/artifact-services/observability"
	"github.com/craggy-io/artifact-services/provenance"
)

func TestReviewListSnapshotExactRevision(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	a := f.artifact(ctx, "data.txt", "x")
	f.revision("A1", a)
	f.revision("A2", a, "A1")
	for _, role := range []string{"", "snapshot"} {
		page, err := f.service.List(ctx, Query{Ref: &provenance.Ref{Revision: "A2"}, Role: role})
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, r := range page.Records {
			found = found || r.ID == "A2"
		}
		if !found {
			t.Fatalf("role %q missed revision A2 itself: %+v", role, page.Records)
		}
	}
}

func TestReviewRecordErrorWrapsPreAppendFailures(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	a := f.artifact(ctx, "data.txt", "x")
	id, err := f.service.Record(ctx, Record{ID: "bad-parent", Revision: &Revision{Artifact: a, Parents: []ID{"missing"}}})
	var stage *RecordError
	if !errors.As(err, &stage) {
		t.Fatalf("err = %v, want RecordError", err)
	}
	if stage.ID != "bad-parent" || stage.Committed || id != "bad-parent" {
		t.Fatalf("stage = %+v id=%s", stage, id)
	}
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("cause lost: %v", err)
	}
	_ = fmt.Sprintf("%v", &RecordError{ID: "x"})
}

func TestReviewClosedAndCursorSentinels(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	if _, err := f.service.List(ctx, Query{Cursor: "bogus.cursor.00"}); !errors.Is(err, ErrCursor) {
		t.Fatalf("bad cursor = %v, want ErrCursor", err)
	}
	if err := f.service.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.List(ctx, Query{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed List = %v, want ErrClosed", err)
	}
	if err := f.service.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestReviewDetailedListRequiresSelectorValidator(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	a := f.artifact(ctx, "data.txt", "x")
	noValidator, err := New(f.artifacts, Options{Root: filepath.Join(t.TempDir(), "lin")})
	if err != nil {
		t.Fatal(err)
	}
	defer noValidator.Close()
	ref := provenance.Ref{Artifact: string(a), Revision: "A1", Row: "row-1"}
	if _, err := noValidator.List(ctx, Query{Ref: &ref}); !errors.Is(err, ErrUnsupportedSelector) {
		t.Fatalf("detailed List without validator = %v", err)
	}
}

func TestReviewIncompatibleQueryCombosRejected(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	ref := provenance.Ref{Revision: "A1"}
	for _, q := range []Query{
		{Ref: &ref, Role: "output", DependencyRole: provenance.Value},
		{Ref: &ref, Role: "from", DependencyRole: provenance.Value},
		{Ref: &ref, Role: "to", DependencyRole: provenance.Value},
		{Activity: "act", Kind: "activity"},
		{Activity: "act", ActivityKind: "import"},
	} {
		if _, err := f.service.List(ctx, q); !errors.Is(err, ErrInvalid) {
			t.Fatalf("query %+v = %v, want ErrInvalid", q, err)
		}
	}
}

func reviewEvidence(t *testing.T, f *fixture, ctx context.Context, mappings []provenance.Mapping) artifacts.ID {
	t.Helper()
	var data bytes.Buffer
	enc := json.NewEncoder(&data)
	for _, m := range mappings {
		if err := enc.Encode(m); err != nil {
			t.Fatal(err)
		}
	}
	return f.artifact(observability.WithInternal(ctx), "mappings.ndjson", data.String())
}

func TestReviewCoarseRefFoundViaMappingEndpoints(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	a := f.artifact(ctx, "a.ndjson", "a")
	c := f.artifact(ctx, "c.ndjson", "c")
	f.revision("A1", a)
	f.revision("C1", c)
	output := provenance.Ref{Artifact: string(c), Revision: "C1", Row: "row-c"}
	coarse := provenance.Ref{Artifact: string(a), Revision: "A1"}
	evidenceID := reviewEvidence(t, f, ctx, []provenance.Mapping{{Output: output, Inputs: []provenance.Dependency{{Ref: coarse, Role: provenance.Value}}}})
	coverage := provenance.Coverage{Rows: provenance.Complete, Columns: provenance.Complete, Values: provenance.Complete, Predicates: provenance.Complete}
	// No direct Inputs/Outputs: the coarse ref occurs only as a mapping input.
	f.record(Record{ID: "mapped-only", Activity: &Activity{Kind: "query", Evidence: []Evidence{{Artifact: evidenceID, Path: "mappings.ndjson", Coverage: coverage}}}})
	page, err := f.service.List(ctx, Query{Ref: &coarse})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, r := range page.Records {
		found = found || r.ID == "mapped-only"
	}
	if !found {
		t.Fatalf("coarse mapping-only ref not found: %+v", page.Records)
	}
}

func TestReviewInternalMappingEdgesHiddenByDefault(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	a := f.artifact(ctx, "a.ndjson", "a")
	c := f.artifact(ctx, "c.ndjson", "c")
	f.revision("A1", a)
	f.revision("C1", c)
	output := provenance.Ref{Artifact: string(c), Revision: "C1", Row: "row-c"}
	input := provenance.Ref{Artifact: string(a), Revision: "A1", Row: "row-a"}
	evidenceID := reviewEvidence(t, f, ctx, []provenance.Mapping{{Output: output, Inputs: []provenance.Dependency{{Ref: input, Role: provenance.Value}}}})
	coverage := provenance.Coverage{Rows: provenance.Complete, Columns: provenance.Complete, Values: provenance.Complete, Predicates: provenance.Complete}
	record := Record{ID: "internal-join", Activity: &Activity{Kind: "query", Evidence: []Evidence{{Artifact: evidenceID, Path: "mappings.ndjson", Coverage: coverage}}}}
	if _, err := f.service.Record(observability.WithInternal(ctx), record); err != nil {
		t.Fatal(err)
	}
	page, err := f.service.Traverse(ctx, TraversalQuery{Root: output})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Edges) != 0 {
		t.Fatalf("internal mapping edges leaked: %+v", page.Edges)
	}
	page, err = f.service.Traverse(ctx, TraversalQuery{Root: output, IncludeInternal: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Edges) != 1 {
		t.Fatalf("internal traversal edges = %+v", page.Edges)
	}
}

func TestReviewSlashIDEdgeIdentityAndDepthTruncation(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	a := f.artifact(ctx, "data.txt", "x")
	f.revision("c", a)
	f.revision("b/c", a, "c")
	f.revision("a", a, "b/c")
	f.revision("a/b", a, "c")
	page, err := f.service.Traverse(ctx, TraversalQuery{Root: provenance.Ref{Revision: "c"}, Kinds: []string{"revision-of"}, Direction: "both"})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Edges) != 3 {
		t.Fatalf("edges = %+v, want 3 (slash-ID collision dropped one)", page.Edges)
	}
	foundA := false
	for _, n := range page.Nodes {
		foundA = foundA || n.Ref.Revision == "a"
	}
	if !foundA {
		t.Fatalf("node a unreachable: %+v", page.Nodes)
	}
	// Single-edge graph at MaxDepth 1 is complete, not depth-truncated.
	f2 := setup(t)
	b := f2.artifact(ctx, "data.txt", "y")
	f2.revision("A1", b)
	f2.revision("A2", b, "A1")
	page, err = f2.service.Traverse(ctx, TraversalQuery{Root: provenance.Ref{Revision: "A2"}, Kinds: []string{"revision-of"}, Direction: "both", MaxDepth: 1})
	if err != nil {
		t.Fatal(err)
	}
	if page.Truncated {
		t.Fatalf("complete graph reported truncated: %+v", page)
	}
	f2.revision("A3", b, "A2")
	page, err = f2.service.Traverse(ctx, TraversalQuery{Root: provenance.Ref{Revision: "A3"}, Kinds: []string{"revision-of"}, Direction: "both", MaxDepth: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !page.Truncated || page.Reason != "depth" {
		t.Fatalf("deeper graph not depth-truncated: %+v", page)
	}
}

func TestReviewTraverseDoesNotMutateCallerKinds(t *testing.T) {
	f := setup(t)
	ctx := context.Background()
	a := f.artifact(ctx, "data.txt", "x")
	f.revision("A1", a)
	kinds := []string{"revision-of", "derived-from"}
	if _, err := f.service.Traverse(ctx, TraversalQuery{Root: provenance.Ref{Revision: "A1"}, Kinds: kinds}); err != nil {
		t.Fatal(err)
	}
	if kinds[0] != "revision-of" || kinds[1] != "derived-from" {
		t.Fatalf("caller Kinds reordered: %v", kinds)
	}
}
