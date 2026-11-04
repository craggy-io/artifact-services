package objects

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"strings"
	"testing"

	"github.com/craggy-io/artifact-services/artifacts"
	"github.com/craggy-io/artifact-services/cas"
)

func TestReviewMaxBytesOverflowRejected(t *testing.T) {
	f := setup(t, Options{})
	if _, err := New(f.c, f.a, Options{MaxBytes: math.MaxInt64}); !errors.Is(err, ErrLimit) {
		t.Fatalf("MaxInt64 MaxBytes = %v, want ErrLimit", err)
	}
	if got := limitPlusOne(math.MaxInt64); got != math.MaxInt64 {
		t.Fatalf("limitPlusOne(MaxInt64) = %d", got)
	}
}

func TestReviewMaxRowBytesBelow4096Enforced(t *testing.T) {
	f := setup(t, Options{MaxRowBytes: 100})
	schema := f.schema(t, tableSchema(false))
	long := `{"id":"r","values":{"key":"` + strings.Repeat("x", 200) + `","amount":1}}`
	data := f.raw(t, "data", long)
	if _, err := f.s.Validate(f.ctx, ValidationRequest{Data: data, Schema: schema, Encoding: NDJSON}); !errors.Is(err, ErrLimit) {
		t.Fatalf("200-byte row under MaxRowBytes=100 = %v, want ErrLimit", err)
	}
}

func TestReviewRatValuePrecisionScopes(t *testing.T) {
	// 40 decimal places: exact addition/SUM allowed, division capped at 32.
	f := setup(t, Options{})
	d := SchemaDefinition{Version: SchemaVersion, Model: Table, Columns: []Column{{ID: "v", Name: "v", Shape: Shape{Type: "number"}}}}
	schema := f.schema(t, d)
	v := "0." + strings.Repeat("0", 39) + "1"
	data := f.typed(t, schema, NDJSON, `{"id":"r","values":{"v":`+v+`}}`)
	q := QueryRequest{Language: SQL, Text: "SELECT t.v + 0 AS v FROM t", Inputs: []Input{{Alias: "t", Artifact: data, Revision: "r1"}}}
	stream, err := f.s.Query(f.ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	rows := collect(t, stream)
	if len(rows) != 1 || rows[0].Values["v"] == nil {
		t.Fatalf("rows = %+v", rows)
	}
	q.Text = "SELECT t.v / 3 AS v FROM t"
	if _, err := f.s.Query(f.ctx, q); err == nil {
		t.Fatal("nonterminating division accepted")
	}
	q.Text = "SELECT t.v / 10 AS v FROM t"
	if _, err := f.s.Query(f.ctx, q); !errors.Is(err, ErrLimit) {
		t.Fatalf("41-place division = %v, want ErrLimit", err)
	}
}

func TestReviewGroupedQualifiedSpellingAndAliasCase(t *testing.T) {
	f := setup(t, Options{})
	schema := f.schema(t, tableSchema(false))
	data := f.typed(t, schema, NDJSON, "{\"id\":\"a\",\"values\":{\"key\":\"x\",\"amount\":1}}\n{\"id\":\"b\",\"values\":{\"key\":\"x\",\"amount\":2}}")
	q := QueryRequest{Language: SQL, Text: "SELECT label AS Key, COUNT(*) AS Amount FROM t GROUP BY t.label", Inputs: []Input{{Alias: "t", Artifact: data, Revision: "r1"}}}
	stream, err := f.s.Query(f.ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	rows := collect(t, stream)
	if len(rows) != 1 {
		t.Fatalf("rows = %+v", rows)
	}
	if _, ok := rows[0].Values["Key"]; !ok {
		t.Fatalf("alias case not preserved: %+v", rows[0].Values)
	}
	if _, ok := rows[0].Values["Amount"]; !ok {
		t.Fatalf("alias case not preserved: %+v", rows[0].Values)
	}
}

func TestReviewNullableEnumConstrainsNull(t *testing.T) {
	sh := Shape{Type: "string", Nullable: true, Enum: []json.RawMessage{json.RawMessage(`"a"`)}}
	if err := checkValue(nil, sh, "x"); err == nil {
		t.Fatal("null accepted outside enum")
	}
	sh.Enum = []json.RawMessage{json.RawMessage(`"a"`), json.RawMessage(`null`)}
	if err := checkValue(nil, sh, "x"); err != nil {
		t.Fatalf("enumerated null rejected: %v", err)
	}
}

func TestReviewCancelledAggregateAborts(t *testing.T) {
	f := setup(t, Options{})
	schema := f.schema(t, tableSchema(false))
	var lines []string
	for i := 0; i < 50; i++ {
		lines = append(lines, `{"id":"r`+strings.Repeat("x", i+1)+`","values":{"key":"x","amount":1}}`)
	}
	data := f.typed(t, schema, NDJSON, strings.Join(lines, "\n"))
	ctx, cancel := context.WithCancel(f.ctx)
	cancel()
	q := QueryRequest{Language: SQL, Text: "SELECT SUM(t.amount) AS amount FROM t", Inputs: []Input{{Alias: "t", Artifact: data, Revision: "r1"}}}
	if _, err := f.s.Query(ctx, q); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled query = %v", err)
	}
}

func TestReviewEvaluateAggregateHonorsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	agg := &expression{kind: "aggregate", text: "count", left: &expression{kind: "star"}}
	group := []tuple{{rows: map[string]boundRow{}}, {rows: map[string]boundRow{}}}
	if _, err := evaluate(ctx, agg, tuple{rows: map[string]boundRow{}}, group, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("aggregate with cancelled ctx = %v", err)
	}
}

type failingCAS struct{ cas.Store }

func (failingCAS) Put(context.Context, io.Reader) (cas.PutResult, error) {
	return cas.PutResult{ID: cas.ID("sha256:" + strings.Repeat("0", 64)), Size: 1},
		&cas.PutError{Committed: true, Err: errors.New("staging cleanup")}
}

func TestReviewPublicationErrorCommittedFlag(t *testing.T) {
	f := setup(t, Options{})
	svc, err := New(failingCAS{f.c}, f.a, Options{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = svc.DefineSchema(f.ctx, tableSchema(false))
	var pub *PublicationError
	if !errors.As(err, &pub) {
		t.Fatalf("err = %v, want PublicationError", err)
	}
	if !pub.ContentCommitted || pub.Content == "" || pub.ArtifactRegistered {
		t.Fatalf("publication stage = %+v", pub)
	}
}

func TestReviewRegisteredFlagMapping(t *testing.T) {
	if !registered("sha256:x", nil) {
		t.Fatal("success not registered")
	}
	if registered("", nil) {
		t.Fatal("empty ID registered")
	}
	committed := &artifacts.PublishError{Registered: true, Err: errors.New("cleanup")}
	if !registered("sha256:x", committed) {
		t.Fatal("committed PublishError not registered")
	}
	uncommitted := &artifacts.PublishError{Registered: false, Err: errors.New("early")}
	if registered("sha256:x", uncommitted) {
		t.Fatal("uncommitted PublishError registered")
	}
	pub := &PublicationError{ArtifactRegistered: true, Err: errors.New("later")}
	if !registered("sha256:x", pub) {
		t.Fatal("committed PublicationError not registered")
	}
}
