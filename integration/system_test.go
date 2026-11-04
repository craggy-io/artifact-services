package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/craggy-io/artifact-services/artifacts"
	"github.com/craggy-io/artifact-services/cas"
	"github.com/craggy-io/artifact-services/lineage"
	"github.com/craggy-io/artifact-services/objects"
	"github.com/craggy-io/artifact-services/observability"
	"github.com/craggy-io/artifact-services/provenance"
	"github.com/craggy-io/artifact-services/storage"
)

// eventLog is a concurrency-safe Observer sink: callbacks may run
// concurrently across spans, so the example must synchronize them.
type eventLog struct {
	mu     sync.Mutex
	events []observability.Event
}

func (l *eventLog) add(e observability.Event) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, e)
}
func (l *eventLog) snapshot() []observability.Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]observability.Event(nil), l.events...)
}

type system struct {
	ctx       context.Context
	cas       *cas.Service
	artifacts *artifacts.Service
	objects   *objects.Service
	lineage   *lineage.Service
	metrics   *observability.Collector
	events    *eventLog
}

func newSystem(t *testing.T) *system {
	t.Helper()
	root := t.TempDir()
	blobs, err := storage.NewFilesystem(filepath.Join(root, "blobs"))
	if err != nil {
		t.Fatal(err)
	}
	content, err := cas.New(blobs, cas.Options{})
	if err != nil {
		t.Fatal(err)
	}
	a, err := artifacts.New(content, artifacts.Options{Root: filepath.Join(root, "catalog"), IndexedMetadata: []string{"kind"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := a.Close(); err != nil {
			t.Error(err)
		}
	})
	o, err := objects.New(content, a, objects.Options{})
	if err != nil {
		t.Fatal(err)
	}
	l, err := lineage.New(a, lineage.Options{Root: filepath.Join(root, "lineage"), ValidateSelector: o.ValidateRef})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := l.Close(); err != nil {
			t.Error(err)
		}
	})
	metrics := new(observability.Collector)
	events := &eventLog{}
	ctx := observability.With(context.Background(), observability.Options{Collector: metrics, Observer: events.add})
	return &system{ctx, content, a, o, l, metrics, events}
}
func (s *system) file(t *testing.T, ctx context.Context, path string, data []byte, kind string) artifacts.ID {
	t.Helper()
	put, err := s.cas.Put(ctx, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	metadata := map[string]json.RawMessage{}
	if kind != "" {
		b, _ := json.Marshal(kind)
		metadata["kind"] = b
	}
	id, err := s.artifacts.Publish(ctx, artifacts.Manifest{Version: 1, Entries: []artifacts.Entry{{Path: path, Content: put.ID, Size: put.Size}}, Metadata: metadata})
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func (s *system) schema(t *testing.T, columns ...objects.Column) artifacts.ID {
	t.Helper()
	id, err := s.objects.DefineSchema(s.ctx, objects.SchemaDefinition{Version: objects.SchemaVersion, Model: objects.Table, Columns: columns})
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func (s *system) table(t *testing.T, schema artifacts.ID, rows string) artifacts.ID {
	t.Helper()
	raw := s.file(t, s.ctx, "data.ndjson", []byte(rows), "source")
	id, err := s.objects.Publish(s.ctx, objects.PublishRequest{ValidationRequest: objects.ValidationRequest{Data: raw, Schema: schema, Encoding: objects.NDJSON}, Metadata: map[string]json.RawMessage{"kind": json.RawMessage(`"table"`)}})
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func (s *system) record(t *testing.T, r lineage.Record) {
	t.Helper()
	if _, err := s.lineage.Record(s.ctx, r); err != nil {
		t.Fatal(err)
	}
}
func (s *system) revision(t *testing.T, id string, artifact artifacts.ID, parents ...lineage.ID) {
	t.Helper()
	s.record(t, lineage.Record{ID: lineage.ID(id), Revision: &lineage.Revision{Artifact: artifact, Parents: parents}})
}

func TestAllLayersJoinSchemaEvolutionAndProvenance(t *testing.T) {
	s := newSystem(t)
	apk := s.file(t, s.ctx, "app.apk", []byte("PK\x03\x04 apk fixture"), "apk")
	s.record(t, lineage.Record{ID: "import-apk", Activity: &lineage.Activity{Kind: "import", Outputs: []provenance.Ref{{Artifact: string(apk)}}, Actor: "user"}})
	// Identical retry is idempotent; a conflicting record for the same ID is rejected.
	s.record(t, lineage.Record{ID: "import-apk", Activity: &lineage.Activity{Kind: "import", Outputs: []provenance.Ref{{Artifact: string(apk)}}, Actor: "user"}})
	if _, err := s.lineage.Record(s.ctx, lineage.Record{ID: "import-apk", Activity: &lineage.Activity{Kind: "import", Outputs: []provenance.Ref{{Artifact: string(apk)}}, Actor: "other"}}); !errors.Is(err, lineage.ErrConflict) {
		t.Fatalf("conflicting record = %v, want ErrConflict", err)
	}
	s.revision(t, "apk-1", apk)
	schema := s.schema(t, objects.Column{ID: "region", Name: "region", Shape: objects.Shape{Type: "string"}}, objects.Column{ID: "amount", Name: "amount", Shape: objects.Shape{Type: "integer"}})
	a := s.table(t, schema, "{\"id\":\"a1\",\"values\":{\"region\":\"west\",\"amount\":2}}\n{\"id\":\"a2\",\"values\":{\"region\":\"west\",\"amount\":3}}\n")
	b := s.table(t, schema, "{\"id\":\"b1\",\"values\":{\"region\":\"west\",\"amount\":10}}\n{\"id\":\"b2\",\"values\":{\"region\":\"west\",\"amount\":20}}\n")
	s.revision(t, "A-1", a)
	s.revision(t, "B-1", b)
	s.revision(t, "A-independent", a)
	// New schema identity changes display names but retains explicit column IDs.
	schema2 := s.schema(t, objects.Column{ID: "region", Name: "region", Shape: objects.Shape{Type: "string"}}, objects.Column{ID: "amount", Name: "quantity", Shape: objects.Shape{Type: "integer"}})
	if schema == schema2 {
		t.Fatal("schema evolution lost")
	}
	s.revision(t, "schema-1", schema)
	s.record(t, lineage.Record{ID: "schema-evolution", Activity: &lineage.Activity{Kind: "schema-change", Inputs: []provenance.Ref{{Artifact: string(schema), Revision: "schema-1"}}, Outputs: []provenance.Ref{{Artifact: string(schema2)}}, Metadata: map[string]json.RawMessage{"column_mappings": json.RawMessage(`{"region":"region","amount":"amount"}`)}}})
	s.record(t, lineage.Record{ID: "schema-2", Revision: &lineage.Revision{Artifact: schema2, Parents: []lineage.ID{"schema-1"}, Activity: "schema-evolution"}})
	sourceClaim, err := s.artifacts.ValidatedClaim(s.ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	claimedSchema, source, validator, encoding, valid := sourceClaim.Fields()
	if !valid || artifacts.ID(claimedSchema) != schema || validator != objects.ValidatorVersion || encoding != objects.NDJSON {
		t.Fatalf("validated claim fields = %q %q %q %q valid=%t", claimedSchema, source, validator, encoding, valid)
	}
	migrated, err := s.objects.Publish(s.ctx, objects.PublishRequest{ValidationRequest: objects.ValidationRequest{Data: artifacts.ID(source), Schema: schema2, Encoding: objects.NDJSON}})
	if err != nil {
		t.Fatal(err)
	}
	s.revision(t, "A-2", migrated, "A-1")
	query := objects.QueryRequest{Language: objects.SQL, Text: "SELECT a.region AS region, SUM(a.amount + b.amount) AS total FROM a INNER JOIN b ON a.region = b.region WHERE a.amount > :min GROUP BY a.region", Inputs: []objects.Input{{Alias: "a", Artifact: a, Revision: "A-1"}, {Alias: "b", Artifact: b, Revision: "B-1"}}, Parameters: map[string]any{"min": 1}, RequireComplete: true}
	stream, err := s.objects.Query(s.ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	row, err := stream.Next()
	if err != nil {
		t.Fatal(err)
	}
	numeric, err := json.Marshal(row.Values["total"])
	if err != nil || string(numeric) != "70" {
		t.Fatalf("many-to-many aggregate: %s %v", numeric, err)
	}
	if _, err = stream.Next(); err != io.EOF {
		t.Fatal("expected one group", err)
	}
	if stream.Coverage().Rows != provenance.Complete {
		t.Fatal(stream.Coverage())
	}
	if err = stream.Close(); err != nil {
		t.Fatal(err)
	}
	inputs := map[string]bool{}
	predicate := false
	for _, d := range row.Dependencies {
		inputs[d.Ref.Revision+"/"+d.Ref.Row] = true
		if d.Role == provenance.Predicate {
			predicate = true
		}
	}
	for _, id := range []string{"A-1/a1", "A-1/a2", "B-1/b1", "B-1/b2"} {
		if !inputs[id] {
			t.Errorf("missing group/join dependency %s: %+v", id, row)
		}
	}
	if !predicate {
		t.Fatal("predicate dependencies lost")
	}
	if len(row.Columns["total"]) == 0 {
		t.Fatal("column expression mapping missing")
	}
	outputSchema := s.schema(t, objects.Column{ID: "region", Name: "region", Shape: objects.Shape{Type: "string"}}, objects.Column{ID: "total", Name: "total", Shape: objects.Shape{Type: "integer"}})
	savedView, err := s.objects.SaveView(s.ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	saved, err := s.objects.GetView(s.ctx, savedView)
	if err != nil || saved.Inputs[0].Revision != "A-1" {
		t.Fatal(saved, err)
	}
	materialized, err := s.objects.Materialize(s.ctx, objects.MaterializeRequest{Query: query, Schema: outputSchema, OutputRevision: "C-1"})
	if err != nil {
		t.Fatal(err)
	}
	s.revision(t, "C-1", materialized.Artifact)
	s.record(t, lineage.Record{ID: "join-1", Activity: &lineage.Activity{Kind: "query", Inputs: []provenance.Ref{{Artifact: string(a), Revision: "A-1"}, {Artifact: string(b), Revision: "B-1"}}, Outputs: []provenance.Ref{{Artifact: string(materialized.Artifact), Revision: "C-1"}}, Evidence: []lineage.Evidence{{Artifact: materialized.Evidence, Path: materialized.EvidencePath, Coverage: materialized.Coverage}}}})
	root := provenance.Ref{Artifact: string(materialized.Artifact), Revision: "C-1", Row: row.ID}
	page, err := s.lineage.Traverse(s.ctx, lineage.TraversalQuery{Root: root, Limit: 1, MaxDepth: 4, MaxNodes: 20, MaxResults: 100})
	if err != nil {
		t.Fatal(err)
	}
	edges := append([]lineage.Edge(nil), page.Edges...)
	for page.NextCursor != "" {
		page, err = s.lineage.Traverse(s.ctx, lineage.TraversalQuery{Root: root, Limit: 1, MaxDepth: 4, MaxNodes: 20, MaxResults: 100, Cursor: page.NextCursor})
		if err != nil {
			t.Fatal(err)
		}
		edges = append(edges, page.Edges...)
	}
	if len(edges) < 4 {
		t.Fatalf("missing detailed provenance: %+v", edges)
	}
	for _, edge := range edges {
		if edge.To.Revision == "A-independent" {
			t.Fatal("content equality collapsed history")
		}
	}
	columnPage, err := s.lineage.Traverse(s.ctx, lineage.TraversalQuery{Root: provenance.Ref{Artifact: string(materialized.Artifact), Revision: "C-1", Schema: string(outputSchema), Column: "total"}, DependencyRole: provenance.Value, MaxDepth: 4, MaxNodes: 20, MaxResults: 100})
	if err != nil {
		t.Fatal(err)
	}
	if len(columnPage.Edges) < 2 {
		t.Fatalf("column lineage missing: %+v", columnPage)
	}
	expression := false
	for _, edge := range columnPage.Edges {
		if edge.Expression != "" {
			expression = true
		}
		if edge.To.Column != "amount" {
			t.Fatalf("unexpected value column: %+v", edge)
		}
	}
	if !expression {
		t.Fatal("column expression evidence missing")
	}
	revision, err := s.lineage.Get(s.ctx, "C-1")
	if err != nil || len(revision.Revision.Parents) != 0 {
		t.Fatal("derivation became revision", revision, err)
	}
	catalog, err := s.artifacts.Search(s.ctx, artifacts.CatalogQuery{SchemaID: outputSchema})
	if err != nil || len(catalog.Items) != 1 || catalog.Items[0].ID != materialized.Artifact {
		t.Fatal(catalog, err)
	}
	hidden, err := s.artifacts.Search(s.ctx, artifacts.CatalogQuery{ID: materialized.Evidence})
	if err != nil || len(hidden.Items) != 0 {
		t.Fatal("mapping leaked", hidden, err)
	}
	diagnostic, err := s.artifacts.Search(s.ctx, artifacts.CatalogQuery{ID: materialized.Evidence, IncludeInternal: true})
	if err != nil || len(diagnostic.Items) != 1 {
		t.Fatal(diagnostic, err)
	}
	observed := s.events.snapshot()
	if len(observed) == 0 {
		t.Fatal("no events observed; Observer wiring broken")
	}
	finals := 0
	for _, event := range observed {
		if event.Category != observability.User {
			t.Fatal("internal event leaked")
		}
		if event.Final {
			finals++
		}
	}
	if finals == 0 {
		t.Fatal("no final events observed")
	}
	user := s.metrics.Snapshot(observability.SnapshotOptions{})
	all := s.metrics.Snapshot(observability.SnapshotOptions{IncludeInternal: true})
	scope := observability.Scope{Layer: "storage", Operation: "put"}
	if all[scope].Completed <= user[scope].Completed {
		t.Fatal("internal bucket missing")
	}
}

func TestTypedValidationCannotBeSpoofedAndJSONQueries(t *testing.T) {
	s := newSystem(t)
	shape := objects.Shape{Type: "object", Properties: map[string]objects.Shape{"name": {Type: "string"}, "count": {Type: "integer"}}, Required: []string{"name", "count"}}
	schema, err := s.objects.DefineSchema(s.ctx, objects.SchemaDefinition{Version: objects.SchemaVersion, Model: objects.JSON, JSON: &shape})
	if err != nil {
		t.Fatal(err)
	}
	source := s.file(t, s.ctx, "doc.json", []byte(`{"name":"hello","count":3}`), "json")
	typed, err := s.objects.Publish(s.ctx, objects.PublishRequest{ValidationRequest: objects.ValidationRequest{Data: source, Schema: schema, Encoding: objects.JSONEncoding}})
	if err != nil {
		t.Fatal(err)
	}
	m, err := s.artifacts.Get(s.ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	m.Metadata = map[string]json.RawMessage{"objects.schema": json.RawMessage(fmt.Sprintf("%q", schema))}
	if _, err = s.artifacts.Publish(s.ctx, m); err == nil {
		t.Fatal("reserved type claim accepted")
	}
	bad := s.file(t, s.ctx, "doc.json", []byte(`{"name":3,"count":3}`), "json")
	if _, err = s.objects.Publish(s.ctx, objects.PublishRequest{ValidationRequest: objects.ValidationRequest{Data: bad, Schema: schema, Encoding: objects.JSONEncoding}}); !errors.Is(err, objects.ErrValidation) {
		t.Fatal(err)
	}
	s.revision(t, "json-1", typed)
	stream, err := s.objects.Query(s.ctx, objects.QueryRequest{Language: objects.JSONPointer, Text: "/name", Inputs: []objects.Input{{Alias: "document", Artifact: typed, Revision: "json-1"}}})
	if err != nil {
		t.Fatal(err)
	}
	row, err := stream.Next()
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(row.Values)
	if !strings.Contains(string(b), "hello") {
		t.Fatal(string(b))
	}
	if _, err = stream.Next(); err != io.EOF {
		t.Fatal("expected EOF after one JSON row", err)
	}
	if coverage := stream.Coverage(); coverage.Rows != provenance.Unsupported {
		t.Fatalf("JSON coverage = %+v, want Rows unsupported", coverage)
	}
	if len(row.Dependencies) != 1 || row.Dependencies[0].Ref.Artifact != string(typed) || row.Dependencies[0].Ref.Revision != "json-1" {
		t.Fatalf("JSON document dependency = %+v", row.Dependencies)
	}
	if err = stream.Close(); err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(s.ctx)
	cancel()
	if _, err = s.artifacts.Search(cancelled, artifacts.CatalogQuery{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
