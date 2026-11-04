package objects

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/craggy-io/artifact-services/artifacts"
	"github.com/craggy-io/artifact-services/cas"
	"github.com/craggy-io/artifact-services/provenance"
	"github.com/craggy-io/artifact-services/storage"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type fixture struct {
	s   *Service
	a   *artifacts.Service
	c   cas.Store
	ctx context.Context
}

func setup(t *testing.T, options Options) *fixture {
	t.Helper()
	root := t.TempDir()
	disk, err := storage.NewFilesystem(filepath.Join(root, "blobs"))
	if err != nil {
		t.Fatal(err)
	}
	c, err := cas.New(disk, cas.Options{})
	if err != nil {
		t.Fatal(err)
	}
	a, err := artifacts.New(c, artifacts.Options{Root: filepath.Join(root, "catalog")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := a.Close(); err != nil {
			t.Error(err)
		}
	})
	s, err := New(c, a, options)
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{s, a, c, context.Background()}
}
func (f *fixture) schema(t *testing.T, d SchemaDefinition) artifacts.ID {
	t.Helper()
	id, err := f.s.DefineSchema(f.ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func (f *fixture) raw(t *testing.T, path, text string) artifacts.ID {
	t.Helper()
	p, err := f.c.Put(f.ctx, strings.NewReader(text))
	if err != nil {
		t.Fatal(err)
	}
	id, err := f.a.Publish(f.ctx, artifacts.Manifest{Version: 1, Entries: []artifacts.Entry{{Path: path, Content: p.ID, Size: p.Size}}})
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func (f *fixture) typed(t *testing.T, schema artifacts.ID, encoding, text string) artifacts.ID {
	t.Helper()
	raw := f.raw(t, "data", text)
	id, err := f.s.Publish(f.ctx, PublishRequest{ValidationRequest: ValidationRequest{Data: raw, Schema: schema, Encoding: encoding}})
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func tableSchema(unique bool) SchemaDefinition {
	return SchemaDefinition{Version: SchemaVersion, Model: Table, Columns: []Column{{ID: "key", Name: "label", Shape: Shape{Type: "string"}}, {ID: "amount", Name: "amount", Shape: Shape{Type: "integer"}, Unique: unique}}}
}
func collect(t *testing.T, stream ResultStream) []ResultRow {
	t.Helper()
	defer stream.Close()
	var rows []ResultRow
	for {
		row, err := stream.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, row)
	}
	return rows
}
func TestDefinitionAndDataValidation(t *testing.T) {
	f := setup(t, Options{})
	bad := []SchemaDefinition{{Version: "unknown", Model: Table}, {Version: SchemaVersion, Model: Table}, {Version: SchemaVersion, Model: JSON, JSON: &Shape{Type: "integer", Enum: []json.RawMessage{json.RawMessage(`"bad"`)}}}, {Version: SchemaVersion, Model: JSON, JSON: &Shape{Type: "array"}}, {Version: SchemaVersion, Model: JSON, JSON: &Shape{Type: "object", Required: []string{"absent"}}}}
	for _, d := range bad {
		if _, err := f.s.DefineSchema(f.ctx, d); !errors.Is(err, ErrSchema) {
			t.Fatalf("definition accepted: %+v %v", d, err)
		}
	}
	schema := f.schema(t, tableSchema(true))
	for _, text := range []string{`{"id":"r","values":{"key":"x","amount":null}}`, `{"id":"r","values":{"key":"x","amount":1.0}}`, `{"id":"r","values":{"key":"x","amount":"1"}}`, `{"id":"","values":{"key":"x","amount":1}}`, `{"id":"r","values":{"key":"x","amount":1,"extra":1}}`, `{"id":"r","id":"s","values":{"key":"x","amount":1}}`, "{\"id\":\"r\",\"values\":{\"key\":\"x\",\"amount\":1}}\n{\"id\":\"r\",\"values\":{\"key\":\"x\",\"amount\":2}}", "{\"id\":\"r\",\"values\":{\"key\":\"x\",\"amount\":1}}\n{\"id\":\"s\",\"values\":{\"key\":\"x\",\"amount\":1}}"} {
		data := f.raw(t, "data", text)
		report, err := f.s.Validate(f.ctx, ValidationRequest{Data: data, Schema: schema, Encoding: NDJSON})
		if !errors.Is(err, ErrValidation) || report.Valid || report.Coverage.Rows == provenance.Complete {
			t.Fatalf("invalid data accepted: %s %+v %v", text, report, err)
		}
	}
	// Binary64-bound configuration must not round a larger exact integer down.
	max := float64(9007199254740992)
	d := tableSchema(false)
	d.Columns[1].Shape.Maximum = &max
	bounded := f.schema(t, d)
	data := f.raw(t, "data", `{"id":"r","values":{"key":"x","amount":9007199254740993}}`)
	if _, err := f.s.Validate(f.ctx, ValidationRequest{Data: data, Schema: bounded, Encoding: NDJSON}); !errors.Is(err, ErrValidation) {
		t.Fatal(err)
	}
}
func TestExactArithmeticJoinsGroupsAndHistories(t *testing.T) {
	f := setup(t, Options{})
	schema := f.schema(t, tableSchema(false))
	a := f.typed(t, schema, NDJSON, "{\"id\":\"a1\",\"values\":{\"key\":\"x\",\"amount\":2}}\n{\"id\":\"a2\",\"values\":{\"key\":\"x\",\"amount\":3}}\n")
	b := f.typed(t, schema, NDJSON, "{\"id\":\"b1\",\"values\":{\"key\":\"x\",\"amount\":10}}\n{\"id\":\"b2\",\"values\":{\"key\":\"x\",\"amount\":20}}\n")
	q := QueryRequest{Language: SQL, Text: "SELECT a.label AS key, SUM(a.amount + b.amount) AS total, COUNT(*) AS count FROM a INNER JOIN b ON a.label = b.label WHERE a.amount > :min GROUP BY a.label", Inputs: []Input{{Alias: "a", Artifact: a, Revision: "A-1"}, {Alias: "b", Artifact: b, Revision: "B-1"}}, Parameters: map[string]any{"min": 1}, RequireComplete: true}
	stream, err := f.s.Query(f.ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if stream.Coverage().Rows != provenance.Incomplete {
		t.Fatal("premature completeness")
	}
	rows := collect(t, stream)
	if len(rows) != 1 || rows[0].Values["total"] != json.Number("70") || rows[0].Values["count"] != json.Number("4") {
		t.Fatal(rows)
	}
	if stream.Coverage().Rows != provenance.Complete {
		t.Fatal(stream.Coverage())
	}
	roles := map[string]bool{}
	ids := map[string]bool{}
	for _, d := range rows[0].Dependencies {
		roles[d.Role] = true
		ids[d.Ref.Revision+"/"+d.Ref.Row] = true
	}
	if !roles[provenance.Predicate] || !roles[provenance.Control] || !ids["A-1/a1"] || !ids["B-1/b2"] {
		t.Fatal(rows[0])
	}
	// Same bytes in a different history change provenance and output identity.
	q.Inputs[0].Revision = "A-other"
	other, err := f.s.Query(f.ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if collect(t, other)[0].ID == rows[0].ID {
		t.Fatal("histories collapsed")
	}
	bigData := f.typed(t, schema, NDJSON, `{"id":"big","values":{"key":"x","amount":9007199254740993}}`)
	exact, err := f.s.Query(f.ctx, QueryRequest{Language: SQL, Text: "SELECT t.amount + 1 AS amount FROM t", Inputs: []Input{{Alias: "t", Artifact: bigData, Revision: "large"}}})
	if err != nil {
		t.Fatal(err)
	}
	if collect(t, exact)[0].Values["amount"] != json.Number("9007199254740994") {
		t.Fatal("integer precision lost")
	}
}
func TestQueryRejectionsAndLimits(t *testing.T) {
	f := setup(t, Options{MaxJoinPairs: 1})
	schema := f.schema(t, tableSchema(false))
	data := f.typed(t, schema, NDJSON, "{\"id\":\"r1\",\"values\":{\"key\":\"x\",\"amount\":1}}\n{\"id\":\"r2\",\"values\":{\"key\":\"x\",\"amount\":2}}\n")
	for _, sql := range []string{"SELECT secret(t.amount) AS v FROM t", "SELECT t.amount AS amount FROM t ORDER BY t.amount", "SELECT SUM(t.amount) + t.amount AS bad FROM t", "SELECT t.nope AS x FROM t", "SELECT t.amount AS x FROM t WHERE t.amount = :missing", "DELETE FROM t", "SELECT SUM(COUNT(t.amount)) AS v FROM t"} {
		if _, err := f.s.Query(f.ctx, QueryRequest{Language: SQL, Text: sql, Inputs: []Input{{Alias: "t", Artifact: data, Revision: "T-1"}}, RequireComplete: true}); err == nil {
			t.Fatal("unsupported accepted:", sql)
		}
	}
	if _, err := f.s.Query(f.ctx, QueryRequest{Language: SQL, Text: "SELECT a.amount AS v FROM a INNER JOIN b ON a.label = b.label", Inputs: []Input{{Alias: "a", Artifact: data, Revision: "T-1"}, {Alias: "b", Artifact: data, Revision: "T-2"}}}); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(f.ctx)
	cancel()
	if _, err := f.s.Query(ctx, QueryRequest{Language: SQL, Text: "SELECT t.amount AS v FROM t", Inputs: []Input{{Alias: "t", Artifact: data, Revision: "T-1"}}}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	stream, err := f.s.Query(f.ctx, QueryRequest{Language: SQL, Text: "SELECT t.amount AS v FROM t", Inputs: []Input{{Alias: "t", Artifact: data, Revision: "T-1"}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if stream.Coverage().Rows != provenance.Incomplete {
		t.Fatal("early close complete")
	}
	if _, err := stream.Next(); err != io.ErrClosedPipe {
		t.Fatal(err)
	}
}
func TestNestedJSONAndPointerCoverage(t *testing.T) {
	f := setup(t, Options{})
	shape := Shape{Type: "object", Properties: map[string]Shape{"nested": {Type: "object", Properties: map[string]Shape{"a/b": {Type: "array", Items: &Shape{Type: "integer"}}}, Required: []string{"a/b"}}}, Required: []string{"nested"}}
	schema := f.schema(t, SchemaDefinition{Version: SchemaVersion, Model: JSON, JSON: &shape})
	data := f.typed(t, schema, JSONEncoding, `{"nested":{"a/b":[3,4]}}`)
	q := QueryRequest{Language: JSONPointer, Text: "/nested/a~1b/1", Inputs: []Input{{Alias: "d", Artifact: data, Revision: "D-1"}}}
	stream, err := f.s.Query(f.ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	rows := collect(t, stream)
	if rows[0].Values["value"] != json.Number("4") || stream.Coverage().Rows != provenance.Unsupported {
		t.Fatal(rows, stream.Coverage())
	}
	q.RequireComplete = true
	if _, err := f.s.Query(f.ctx, q); !errors.Is(err, ErrUnsupported) {
		t.Fatal(err)
	}
	bad := f.raw(t, "doc", `{"nested":{"a/b":[3],"unknown":1}}`)
	if _, err := f.s.Publish(f.ctx, PublishRequest{ValidationRequest: ValidationRequest{Data: bad, Schema: schema, Encoding: JSONEncoding}}); !errors.Is(err, ErrValidation) {
		t.Fatal(err)
	}
}
func TestMaterializeMappingsAndTypedClaims(t *testing.T) {
	temps := t.TempDir()
	f := setup(t, Options{TempDir: temps})
	schema := f.schema(t, tableSchema(false))
	data := f.typed(t, schema, NDJSON, `{"id":"r1","values":{"key":"x","amount":3}}`)
	query := QueryRequest{Language: SQL, Text: "SELECT t.label AS key, t.amount * 2 AS amount FROM t", Inputs: []Input{{Alias: "t", Artifact: data, Revision: "T-1"}}, RequireComplete: true}
	view, err := f.s.SaveView(f.ctx, query)
	if err != nil {
		t.Fatal(err)
	}
	saved, err := f.s.GetView(f.ctx, view)
	if err != nil || saved.Inputs[0].Revision != "T-1" {
		t.Fatal(saved, err)
	}
	result, err := f.s.Materialize(f.ctx, MaterializeRequest{Query: query, Schema: schema, OutputRevision: "C-1"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Rows != 1 || result.Coverage.Columns != provenance.Complete {
		t.Fatal(result)
	}
	r, err := f.a.OpenFile(f.ctx, result.Evidence, result.EvidencePath)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	decoder := json.NewDecoder(r)
	rowOnly, columnOnly, cell := false, false, false
	for {
		var m provenance.Mapping
		err := decoder.Decode(&m)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		rowOnly = rowOnly || (m.Output.Row != "" && m.Output.Column == "")
		columnOnly = columnOnly || (m.Output.Row == "" && m.Output.Column == "amount" && m.Expression != "")
		cell = cell || (m.Output.Row != "" && m.Output.Column != "")
	}
	if !rowOnly || !columnOnly || !cell {
		t.Fatal("mapping forms missing")
	}
	if err := f.s.ValidateRef(f.ctx, provenance.Ref{Artifact: string(result.Artifact), Revision: "C-1", Schema: string(schema), Column: "amount"}); err != nil {
		t.Fatal(err)
	}
	if err := f.s.ValidateRef(f.ctx, provenance.Ref{Artifact: string(result.Artifact), Revision: "C-1", Row: "missing"}); !errors.Is(err, ErrValidation) {
		t.Fatal(err)
	}
	m, err := f.a.Get(f.ctx, data)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.a.Publish(f.ctx, m); err == nil {
		t.Fatal("typed claim spoof accepted")
	}
	entries, err := os.ReadDir(temps)
	if err != nil || len(entries) != 0 {
		t.Fatal("temporary leak", entries, err)
	}
}
func TestMaterializeReportsCommittedArtifactWhenEvidenceTooLarge(t *testing.T) {
	f := setup(t, Options{MaxBytes: 256})
	schema := f.schema(t, tableSchema(false))
	data := f.typed(t, schema, NDJSON, `{"id":"r","values":{"key":"x","amount":1}}`)
	out, err := f.s.Materialize(f.ctx, MaterializeRequest{Schema: schema, OutputRevision: "new", Query: QueryRequest{Language: SQL, Text: "SELECT t.label AS key, t.amount AS amount FROM t", Inputs: []Input{{Alias: "t", Artifact: data, Revision: "old"}}, RequireComplete: true}})
	var partial *MaterializeError
	if !errors.Is(err, ErrLimit) || !errors.As(err, &partial) || out.Artifact == "" || partial.Artifact != out.Artifact || out.Evidence != "" {
		t.Fatal(out, err)
	}
	m, err := f.a.Get(f.ctx, out.Artifact)
	if err != nil || len(m.Entries) != 1 {
		t.Fatal(m, err)
	}
}
func TestReturnedResultsAreIsolated(t *testing.T) {
	f := setup(t, Options{})
	schema := f.schema(t, tableSchema(false))
	data := f.typed(t, schema, NDJSON, `{"id":"r","values":{"key":"x","amount":1}}`)
	q := QueryRequest{Language: SQL, Text: "SELECT t.label AS key FROM t", Inputs: []Input{{Alias: "t", Artifact: data, Revision: "old"}}}
	a, err := f.s.Query(f.ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	first := collect(t, a)
	first[0].Values["key"] = "mutated"
	b, err := f.s.Query(f.ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	second := collect(t, b)
	encoded, _ := json.Marshal(second[0].Values)
	if !bytes.Equal(encoded, []byte(`{"key":"x"}`)) {
		t.Fatal(string(encoded))
	}
}
