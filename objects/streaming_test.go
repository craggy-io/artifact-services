package objects

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/craggy-io/artifact-services/provenance"
)

func keyedSchema() SchemaDefinition {
	return SchemaDefinition{Version: SchemaVersion, Model: Table, Key: []string{"tenant", "seq"}, Columns: []Column{
		{ID: "tenant", Name: "tenant", Shape: Shape{Type: "string"}},
		{ID: "seq", Name: "seq", Shape: Shape{Type: "integer"}},
		{ID: "v", Name: "v", Shape: Shape{Type: "string"}},
	}}
}

func TestKeyedRowsDeriveIdentityAndAddressByKey(t *testing.T) {
	f := setup(t, Options{})
	d := keyedSchema()
	schema := f.schema(t, d)
	data := f.typed(t, schema, NDJSON, `{"values":{"tenant":"a","seq":1,"v":"x"}}`+"\n"+`{"values":{"tenant":"a","seq":2,"v":"y"}}`)

	want, err := KeyRowID(d, "a", 2)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := f.s.Rows(f.ctx, data)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	ids := []string{}
	for {
		row, err := stream.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, row.ID)
	}
	if len(ids) != 2 || ids[1] != want {
		t.Fatalf("ids = %v, want second %s", ids, want)
	}
	ref := provenance.Ref{Artifact: string(data), Revision: "r1", Row: want}
	if err := f.s.ValidateRef(f.ctx, ref); err != nil {
		t.Fatalf("row addressed by key: %v", err)
	}
	missing, _ := KeyRowID(d, "a", 3)
	ref.Row = missing
	if err := f.s.ValidateRef(f.ctx, ref); !errors.Is(err, ErrValidation) {
		t.Fatalf("unknown key = %v", err)
	}
}

func TestKeyedRowRejections(t *testing.T) {
	f := setup(t, Options{})
	schema := f.schema(t, keyedSchema())
	validate := func(lines ...string) error {
		data := f.raw(t, "data", strings.Join(lines, "\n"))
		_, err := f.s.Validate(f.ctx, ValidationRequest{Data: data, Schema: schema, Encoding: NDJSON})
		return err
	}
	if err := validate(`{"id":"wrong","values":{"tenant":"a","seq":1,"v":"x"}}`); !errors.Is(err, ErrValidation) {
		t.Fatalf("mismatched id = %v", err)
	}
	if err := validate(`{"values":{"tenant":"a","seq":1,"v":"x"}}`, `{"values":{"tenant":"a","seq":1,"v":"other"}}`); !errors.Is(err, ErrValidation) {
		t.Fatalf("duplicate key = %v", err)
	}
	for name, d := range map[string]SchemaDefinition{
		"unknown":  {Version: SchemaVersion, Model: Table, Key: []string{"nope"}, Columns: keyedSchema().Columns},
		"repeat":   {Version: SchemaVersion, Model: Table, Key: []string{"seq", "seq"}, Columns: keyedSchema().Columns},
		"nullable": {Version: SchemaVersion, Model: Table, Key: []string{"n"}, Columns: []Column{{ID: "n", Name: "n", Shape: Shape{Type: "string", Nullable: true}}}},
		"number":   {Version: SchemaVersion, Model: Table, Key: []string{"n"}, Columns: []Column{{ID: "n", Name: "n", Shape: Shape{Type: "number"}}}},
	} {
		if _, err := f.s.DefineSchema(f.ctx, d); !errors.Is(err, ErrSchema) {
			t.Errorf("%s key = %v, want ErrSchema", name, err)
		}
	}
}

func TestUniquenessIsStillEnforcedAcrossSpill(t *testing.T) {
	f := setup(t, Options{})
	schema := f.schema(t, SchemaDefinition{Version: SchemaVersion, Model: Table, Columns: []Column{{ID: "u", Name: "u", Shape: Shape{Type: "string"}, Unique: true}}})
	lines := []string{}
	for i := range 2000 {
		lines = append(lines, fmt.Sprintf(`{"id":"r%d","values":{"u":"v%d"}}`, i, i))
	}
	if _, err := f.s.Validate(f.ctx, ValidationRequest{Data: f.raw(t, "data", strings.Join(lines, "\n")), Schema: schema, Encoding: NDJSON}); err != nil {
		t.Fatal(err)
	}
	dup := append(append([]string{}, lines...), `{"id":"other","values":{"u":"v7"}}`)
	if _, err := f.s.Validate(f.ctx, ValidationRequest{Data: f.raw(t, "data", strings.Join(dup, "\n")), Schema: schema, Encoding: NDJSON}); !errors.Is(err, ErrValidation) {
		t.Fatalf("duplicate unique value = %v", err)
	}
	repeated := append(append([]string{}, lines...), `{"id":"r5","values":{"u":"fresh"}}`)
	if _, err := f.s.Validate(f.ctx, ValidationRequest{Data: f.raw(t, "data", strings.Join(repeated, "\n")), Schema: schema, Encoding: NDJSON}); !errors.Is(err, ErrValidation) {
		t.Fatalf("duplicate row id = %v", err)
	}
}

func TestSingleTableQueryStreamsAndEnforcesResultLimit(t *testing.T) {
	f := setup(t, Options{})
	limited, err := New(f.c, f.a, Options{MaxResultRows: 3})
	if err != nil {
		t.Fatal(err)
	}
	schema := f.schema(t, SchemaDefinition{Version: SchemaVersion, Model: Table, Columns: []Column{{ID: "n", Name: "n", Shape: Shape{Type: "integer"}}}})
	lines := []string{}
	for i := range 10 {
		lines = append(lines, fmt.Sprintf(`{"id":"r%d","values":{"n":%d}}`, i, i))
	}
	data := f.typed(t, schema, NDJSON, strings.Join(lines, "\n"))
	q := QueryRequest{Language: SQL, Text: "SELECT t.n AS n FROM t", Inputs: []Input{{Alias: "t", Artifact: data, Revision: "r"}}}
	stream, err := limited.Query(f.ctx, q)
	if err != nil {
		t.Fatalf("limit must surface while streaming, not from Query: %v", err)
	}
	defer stream.Close()
	got := 0
	for {
		if _, err = stream.Next(); err != nil {
			break
		}
		got++
	}
	if !errors.Is(err, ErrLimit) || got != 3 {
		t.Fatalf("rows = %d err = %v; want 3 then ErrLimit, never a silent truncation", got, err)
	}
	if stream.Coverage().Rows == provenance.Complete {
		t.Fatal("a limited stream reported complete coverage")
	}
	// A filter that keeps few rows stays inside the limit however large the table is.
	q.Text = "SELECT t.n AS n FROM t WHERE t.n = 4"
	stream2, err := limited.Query(f.ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	defer stream2.Close()
	rows := collect(t, stream2)
	if len(rows) != 1 || stream2.Coverage().Rows != provenance.Complete {
		t.Fatalf("filtered rows = %v coverage = %+v", rows, stream2.Coverage())
	}
}

func TestMaterializeDerivesKeyedOutputIdentity(t *testing.T) {
	f := setup(t, Options{})
	source := f.schema(t, SchemaDefinition{Version: SchemaVersion, Model: Table, Columns: []Column{{ID: "k", Name: "k", Shape: Shape{Type: "string"}}}})
	data := f.typed(t, source, NDJSON, `{"id":"r1","values":{"k":"alpha"}}`)
	d := SchemaDefinition{Version: SchemaVersion, Model: Table, Key: []string{"k"}, Columns: []Column{{ID: "k", Name: "k", Shape: Shape{Type: "string"}}}}
	output := f.schema(t, d)
	out, err := f.s.Materialize(f.ctx, MaterializeRequest{Schema: output, OutputRevision: "out-1", Query: QueryRequest{Language: SQL, Text: "SELECT t.k AS k FROM t", Inputs: []Input{{Alias: "t", Artifact: data, Revision: "r1"}}}})
	if err != nil {
		t.Fatal(err)
	}
	want, _ := KeyRowID(d, "alpha")
	stream, err := f.s.Rows(f.ctx, out.Artifact)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	row, err := stream.Next()
	if err != nil || row.ID != want {
		t.Fatalf("row = %+v err = %v, want ID %s", row, err, want)
	}
}

func TestShapeHelpersAreUsableWithoutAnArtifact(t *testing.T) {
	shape := Shape{Type: "object", Properties: map[string]Shape{"n": {Type: "integer"}}, Required: []string{"n"}}
	if err := ValidateShape(shape); err != nil {
		t.Fatal(err)
	}
	if err := ValidateShape(Shape{Type: "nonsense"}); !errors.Is(err, ErrSchema) {
		t.Fatalf("bad shape = %v", err)
	}
	if err := CheckJSON([]byte(`{"n":1}`), shape); err != nil {
		t.Fatal(err)
	}
	if err := CheckJSON([]byte(`{"n":"one"}`), shape); !errors.Is(err, ErrValidation) {
		t.Fatalf("bad value = %v", err)
	}
	if err := CheckJSON([]byte(`{bad`), shape); !errors.Is(err, ErrValidation) {
		t.Fatalf("bad JSON = %v", err)
	}
}

// TestMillionRowTrial is qualification scenario 8: opt-in because it takes a minute.
func TestMillionRowTrial(t *testing.T) {
	if os.Getenv("ARTIFACT_SERVICES_LARGE_TRIALS") == "" {
		t.Skip("set ARTIFACT_SERVICES_LARGE_TRIALS=1 to run")
	}
	const count = 1_000_000
	f := setup(t, Options{MaxRows: count, MaxBytes: 1 << 32, MaxResultRows: count, MaxDependencies: 1 << 40, TempDir: t.TempDir()})
	s := f.s
	d := SchemaDefinition{Version: SchemaVersion, Model: Table, Key: []string{"seq"}, Columns: []Column{
		{ID: "seq", Name: "seq", Shape: Shape{Type: "integer"}},
		{ID: "v", Name: "v", Shape: Shape{Type: "string"}, Unique: true},
	}}
	schema := f.schema(t, d)
	path := filepath.Join(t.TempDir(), "rows.ndjson")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	buffer := bufio.NewWriter(file)
	for i := range count {
		fmt.Fprintf(buffer, `{"values":{"seq":%d,"v":"value-%d"}}`+"\n", i, i)
	}
	if err := errors.Join(buffer.Flush(), file.Close()); err != nil {
		t.Fatal(err)
	}
	input, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	raw, err := s.publishFile(f.ctx, "data.ndjson", input, nil)
	if err != nil {
		t.Fatal(err)
	}

	peak := peakHeap(func() {
		id, err := s.Publish(f.ctx, PublishRequest{ValidationRequest: ValidationRequest{Data: raw, Schema: schema, Encoding: NDJSON}})
		if err != nil {
			t.Fatal(err)
		}
		stream, err := s.Rows(f.ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		defer stream.Close()
		rows := 0
		for {
			_, err := stream.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			if rows++; rows%50000 == 0 {
			}
		}
		if rows != count {
			t.Fatalf("rows = %d", rows)
		}
		target, _ := KeyRowID(d, 999_999)
		if err := s.ValidateRef(f.ctx, provenance.Ref{Artifact: string(id), Revision: "r", Row: target}); err != nil {
			t.Fatal(err)
		}
		// Streaming non-aggregate SQL over the same table.
		q, err := s.Query(f.ctx, QueryRequest{Language: SQL, Text: "SELECT t.seq AS seq FROM t", Inputs: []Input{{Alias: "t", Artifact: id, Revision: "r"}}})
		if err != nil {
			t.Fatal(err)
		}
		defer q.Close()
		results := 0
		for {
			_, err := q.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			results++
		}
		if results != count {
			t.Fatalf("results = %d", results)
		}
	})
	const bound = 256 << 20
	t.Logf("peak sampled heap = %d MiB", peak>>20)
	if peak > bound {
		t.Fatalf("peak heap %d MiB exceeds %d MiB", peak>>20, bound>>20)
	}
}

// peakHeap samples HeapAlloc continuously while run executes. It includes garbage
// not yet collected, so it overstates live memory but cannot miss a peak.
func peakHeap(run func()) uint64 {
	var peak atomic.Uint64
	done, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		for {
			var stats runtime.MemStats
			runtime.ReadMemStats(&stats)
			if stats.HeapAlloc > peak.Load() {
				peak.Store(stats.HeapAlloc)
			}
			select {
			case <-done:
				return
			case <-time.After(20 * time.Millisecond):
			}
		}
	}()
	run()
	close(done)
	<-stopped
	return peak.Load()
}
