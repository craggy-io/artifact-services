package storage

import (
	"context"
	"crypto/sha256"
	"fmt"
	"github.com/craggy-io/artifact-services/observability"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

type deterministicStream struct{ offset int64 }

func (r *deterministicStream) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = byte((r.offset + int64(i)) % 251)
	}
	r.offset += int64(len(p))
	return len(p), nil
}

// Opt-in: streams one GiB without retaining the blob in memory. This Windows
// trial checks free space on the temp volume before allocating any blob data.
func TestLargeBlob1GiB(t *testing.T) {
	if os.Getenv("ARTIFACT_SERVICES_LARGE_TEST") != "1" {
		t.Skip("opt in with ARTIFACT_SERVICES_LARGE_TEST=1")
	}
	const size int64 = 1 << 30
	out, err := exec.Command("powershell.exe", "-NoProfile", "-Command", "[System.IO.DriveInfo]::new([System.IO.Path]::GetPathRoot([System.IO.Path]::GetTempPath())).AvailableFreeSpace").Output()
	if err != nil {
		t.Fatal("free-space check:", err)
	}
	free, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	if free < 2*size {
		t.Fatalf("need 2 GiB free; available=%d", free)
	}
	s, root := newTestStore(t)
	c := new(observability.Collector)
	finals := map[string]observability.Event{}
	updates := 0
	ctx := observability.With(context.Background(), observability.Options{Collector: c, ProgressInterval: 25 * time.Millisecond, Total: func(scope observability.Scope, key string) (int64, bool) { return size, true }, Observer: func(e observability.Event) {
		updates++
		if e.Final {
			finals[e.Scope.Operation] = e
		}
	}})
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	baseline := stats.HeapAlloc
	peak := baseline
	done := make(chan struct{})
	sampled := make(chan struct{})
	samples := 0
	go func() {
		defer close(sampled)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				var m runtime.MemStats
				runtime.ReadMemStats(&m)
				samples++
				if m.HeapAlloc > peak {
					peak = m.HeapAlloc
				}
			}
		}
	}()
	defer func() {
		close(done)
		<-sampled
		t.Logf("Go HeapAlloc baseline=%.2f MiB sampled_peak=%.2f MiB samples=%d interval=10ms", float64(baseline)/(1<<20), float64(peak)/(1<<20), samples)
	}()
	expected := sha256.New()
	started := time.Now()
	err = s.Put(ctx, "abcd", io.TeeReader(io.LimitReader(&deterministicStream{}, size), expected))
	if err != nil {
		t.Fatal(err)
	}
	writeTime := time.Since(started)
	started = time.Now()
	r, err := s.Open(ctx, "abcd")
	if err != nil {
		t.Fatal(err)
	}
	actual := sha256.New()
	n, copyErr := io.Copy(actual, r)
	closeErr := r.Close()
	if copyErr != nil || closeErr != nil {
		t.Fatal(copyErr, closeErr)
	}
	readTime := time.Since(started)
	if n != size || fmt.Sprintf("%x", actual.Sum(nil)) != fmt.Sprintf("%x", expected.Sum(nil)) {
		t.Fatalf("integrity/count mismatch n=%d", n)
	}
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
	t.Logf("%s %s/%s free_before=%d bytes=%d sha256=%x events=%d", runtime.Version(), runtime.GOOS, runtime.GOARCH, free, size, actual.Sum(nil), updates)
	t.Logf("write=%.3fs %.2f MiB/s read=%.3fs %.2f MiB/s (includes generation/hashing, observation; write includes Sync)", writeTime.Seconds(), 1024/writeTime.Seconds(), readTime.Seconds(), 1024/readTime.Seconds())
}
