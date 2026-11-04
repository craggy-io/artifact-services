package artifacts

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/craggy-io/artifact-services/cas"
	"github.com/craggy-io/artifact-services/storage"
)

type countingCAS struct {
	base  cas.Store
	opens atomic.Int64
}

func (c *countingCAS) Put(ctx context.Context, r io.Reader) (cas.PutResult, error) {
	return c.base.Put(ctx, r)
}
func (c *countingCAS) Open(ctx context.Context, id cas.ID) (io.ReadCloser, error) {
	c.opens.Add(1)
	return c.base.Open(ctx, id)
}
func (c *countingCAS) Exists(ctx context.Context, id cas.ID) (bool, error) {
	return c.base.Exists(ctx, id)
}

func TestReviewPublishVerifiesDistinctContentOnce(t *testing.T) {
	base := t.TempDir()
	blob, err := storage.NewFilesystem(filepath.Join(base, "blobs"))
	if err != nil {
		t.Fatal(err)
	}
	content, err := cas.New(blob, cas.Options{TempDir: base})
	if err != nil {
		t.Fatal(err)
	}
	counting := &countingCAS{base: content}
	service, err := New(counting, Options{Root: filepath.Join(base, "catalog")})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	put, err := content.Put(context.Background(), strings.NewReader("shared"))
	if err != nil {
		t.Fatal(err)
	}
	manifest := Manifest{Entries: []Entry{
		{Path: "a", Content: put.ID, Size: put.Size},
		{Path: "b", Content: put.ID, Size: put.Size},
		{Path: "c", Content: put.ID, Size: put.Size},
	}}
	before := counting.opens.Load()
	if _, err := service.Publish(context.Background(), manifest); err != nil {
		t.Fatal(err)
	}
	if got := counting.opens.Load() - before; got != 1 {
		t.Fatalf("verification opened shared blob %d times, want 1", got)
	}
	bad := Manifest{Entries: []Entry{
		{Path: "a", Content: put.ID, Size: put.Size},
		{Path: "b", Content: put.ID, Size: put.Size + 1},
	}}
	if _, err := service.Publish(context.Background(), bad); err == nil {
		t.Fatal("mismatched duplicate size accepted")
	}
}

func TestReviewPublicCursorAndClosedErrors(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	id := publish(t, f.service, ctx, f.manifest(t, "a.txt", "hello"))
	if _, err := f.service.Search(ctx, CatalogQuery{Cursor: "bogus.cursor.00"}); !errors.Is(err, ErrCursor) {
		t.Fatalf("bad search cursor = %v, want ErrCursor", err)
	}
	if _, err := f.service.ListFiles(ctx, id, FileQuery{Cursor: "bogus.cursor.00"}); !errors.Is(err, ErrCursor) {
		t.Fatalf("bad files cursor = %v, want ErrCursor", err)
	}
	if err := f.service.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.Search(ctx, CatalogQuery{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed Search = %v, want ErrClosed", err)
	}
	if _, err := f.service.Publish(ctx, f.manifest(t, "b.txt", "x")); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed Publish = %v, want ErrClosed", err)
	}
	// Close is idempotent once completed.
	if err := f.service.Close(); err != nil {
		t.Fatal(err)
	}
}
