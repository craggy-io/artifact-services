// Package testutil shares opt-in streaming qualification across layers.
package testutil

import (
	"crypto/sha256"
	"fmt"
	"io"
	"runtime"
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

// LargeRoundTrip verifies a GiB without a whole-file buffer and measures sampled
// Go heap, not exact process working-set peaks. Caller checks space and opts in.
func LargeRoundTrip(t testing.TB, put func(io.Reader) error, open func() (io.ReadCloser, error), validate func(int64, string)) {
	t.Helper()
	const size int64 = 1 << 30
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
		// Enforce the streaming claim: a whole-blob buffer regression (1 GiB)
		// must fail the trial, with generous headroom for runtime noise.
		const maxHeapGrowth = 256 << 20
		if peak > baseline && peak-baseline > maxHeapGrowth {
			t.Errorf("HeapAlloc grew %.2f MiB over baseline during round trip; whole-blob buffering suspected (limit %.2f MiB)", float64(peak-baseline)/(1<<20), float64(maxHeapGrowth)/(1<<20))
		}
	}()
	expected := sha256.New()
	started := time.Now()
	if err := put(io.TeeReader(io.LimitReader(&deterministicStream{}, size), expected)); err != nil {
		t.Fatal(err)
	}
	writeTime := time.Since(started)
	started = time.Now()
	r, err := open()
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
	digest := fmt.Sprintf("%x", actual.Sum(nil))
	if n != size || digest != fmt.Sprintf("%x", expected.Sum(nil)) {
		t.Fatalf("integrity/count mismatch n=%d", n)
	}
	validate(size, digest)
	t.Logf("%s %s/%s bytes=%d sha256=%s", runtime.Version(), runtime.GOOS, runtime.GOARCH, size, digest)
	t.Logf("write=%.3fs %.2f MiB/s read=%.3fs %.2f MiB/s (includes generation/hashing and observation)", writeTime.Seconds(), 1024/writeTime.Seconds(), readTime.Seconds(), 1024/readTime.Seconds())
}
