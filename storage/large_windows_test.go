package storage

import (
	"context"
	"github.com/craggy-io/artifact-services/internal/testutil"
	"github.com/craggy-io/artifact-services/observability"
	"io"
	"testing"
	"time"
)

func TestLargeBlob1GiB(t *testing.T) {
	testutil.RequireLargeTrial(t, 2<<30)
	s, root := newTestStore(t)
	c := new(observability.Collector)
	finals := map[string]observability.Event{}
	updates := 0
	ctx := observability.With(context.Background(), observability.Options{Collector: c, ProgressInterval: 25 * time.Millisecond, Total: func(observability.Scope, string) (int64, bool) { return 1 << 30, true }, Observer: func(e observability.Event) {
		updates++
		if e.Final {
			finals[e.Scope.Operation] = e
		}
	}})
	testutil.LargeRoundTrip(t, func(r io.Reader) error { return s.Put(ctx, "abcd", r) }, func() (io.ReadCloser, error) { return s.Open(ctx, "abcd") }, func(size int64, digest string) {
		for _, op := range []string{"put", "open"} {
			e := finals[op]
			if !e.Final || e.Bytes != size || e.Total != size || e.Err != nil {
				t.Fatalf("final %s: %+v", op, e)
			}
			m := c.Snapshot(observability.SnapshotOptions{})[observability.Scope{Layer: "storage", Operation: op}]
			if m.Bytes != size || m.Completed != 1 || m.Errors != 0 {
				t.Fatal(m)
			}
		}
		if finals["put"].Counters["published"] != 1 || finals["open"].Counters["eof"] != 1 || finals["open"].Counters["closed"] != 1 {
			t.Fatal("wrong terminal semantics")
		}
		assertNoPending(t, root)
		t.Logf("events=%d", updates)
	})
}
