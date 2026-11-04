package integration_test

import (
	"context"
	"github.com/craggy-io/artifact-services/cas"
	"github.com/craggy-io/artifact-services/internal/testutil"
	"github.com/craggy-io/artifact-services/observability"
	"github.com/craggy-io/artifact-services/storage"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLargeCAS1GiB(t *testing.T) {
	testutil.RequireLargeTrial(t, 3<<30)
	root := t.TempDir()
	spool := filepath.Join(root, "spool")
	if err := os.Mkdir(spool, 0700); err != nil {
		t.Fatal(err)
	}
	blobs, err := storage.NewFilesystem(filepath.Join(root, "blobs"))
	if err != nil {
		t.Fatal(err)
	}
	content, err := cas.New(blobs, cas.Options{TempDir: spool})
	if err != nil {
		t.Fatal(err)
	}
	c := new(observability.Collector)
	finals := map[string]observability.Event{}
	ctx := observability.With(context.Background(), observability.Options{Collector: c, ProgressInterval: 25 * time.Millisecond, Observer: func(e observability.Event) {
		if e.Final {
			finals[e.Scope.Layer+"/"+e.Scope.Operation] = e
		}
	}})
	var result cas.PutResult
	testutil.LargeRoundTrip(t, func(r io.Reader) error { var e error; result, e = content.Put(ctx, r); return e }, func() (io.ReadCloser, error) { return content.Open(ctx, result.ID) }, func(size int64, digest string) {
		if result.ID != cas.ID("sha256:"+digest) || result.Size != size || result.Reused {
			t.Fatal(result)
		}
		for _, layer := range []string{"storage", "cas"} {
			for _, op := range []string{"put", "open"} {
				e := finals[layer+"/"+op]
				if !e.Final || e.Bytes != size || e.Err != nil {
					t.Fatal(e)
				}
			}
		}
		if c.Snapshot(observability.SnapshotOptions{})[observability.Scope{Layer: "cas", Operation: "put"}].Counters["published"] != 1 {
			t.Fatal("publication metrics")
		}
		if err := filepath.WalkDir(root, func(path string, e fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if strings.Contains(e.Name(), "pending-") {
				t.Errorf("pending file: %s", path)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})
}
