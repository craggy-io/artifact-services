package stack_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/craggy-io/artifact-services/adapters/stack"
	"github.com/craggy-io/artifact-services/artifacts"
	"github.com/craggy-io/artifact-services/objects"
)

func TestPublishTable(t *testing.T) {
	ctx := context.Background()
	st, err := stack.Open(stack.Options{Root: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	def := stack.TableSchema([]string{"id"},
		objects.Column{ID: "id", Name: "id", Shape: objects.Shape{Type: "string", Nullable: true}},
		objects.Column{ID: "n", Name: "n", Shape: objects.Shape{Type: "integer"}})
	if def.Columns[0].Shape.Nullable || !def.Columns[1].Shape.Nullable {
		t.Fatalf("nullability = %+v", def.Columns)
	}
	for range 2 { // the second publish reuses the memoized schema
		id, err := st.PublishTable(ctx, "t", def, []map[string]any{{"id": "a", "n": 1}, {"id": "b", "n": nil}})
		if err != nil {
			t.Fatal(err)
		}
		if n := rows(t, st, id); n != 2 {
			t.Fatalf("rows = %d", n)
		}
	}
	other := stack.TableSchema([]string{"id"}, objects.Column{ID: "id", Name: "id", Shape: objects.Shape{Type: "string"}})
	if _, err := st.PublishTable(ctx, "t", other, nil); err == nil {
		t.Fatal("a changed column set under one key was accepted")
	}
	if _, err := st.PublishTable(ctx, "t", def, []map[string]any{{"n": 1}}); err == nil {
		t.Fatal("a row without its key was accepted")
	}
}

func rows(t *testing.T, st *stack.Stack, id artifacts.ID) int {
	t.Helper()
	stream, err := st.Objects.Rows(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	for n := 0; ; n++ {
		if _, err := stream.Next(); errors.Is(err, io.EOF) {
			return n
		} else if err != nil {
			t.Fatal(err)
		}
	}
}
