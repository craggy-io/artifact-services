package cas

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/craggy-io/artifact-services/observability"
	"github.com/craggy-io/artifact-services/storage"
)

func newService(t *testing.T) (*Service, *storage.Filesystem, string) {
	t.Helper()
	root := t.TempDir()
	blobs, err := storage.NewFilesystem(filepath.Join(root, "blobs"))
	if err != nil {
		t.Fatal(err)
	}
	staging := filepath.Join(root, "staging")
	if err := os.Mkdir(staging, 0700); err != nil {
		t.Fatal(err)
	}
	service, err := New(blobs, Options{TempDir: staging})
	if err != nil {
		t.Fatal(err)
	}
	return service, blobs, staging
}
func assertNoStaging(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("staging remains: %v", entries)
	}
}
func readContent(t *testing.T, service Store, id ID) []byte {
	t.Helper()
	r, err := service.Open(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(r)
	if closeErr := r.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if err != nil {
		t.Fatal(err)
	}
	return data
}
func idFor(data []byte) ID { return ID(fmt.Sprintf("sha256:%x", sha256.Sum256(data))) }

func TestRoundTripAndVerifiedReuse(t *testing.T) {
	for _, data := range [][]byte{nil, {0, 255, 1}, bytes.Repeat([]byte("stream"), 10000)} {
		s, _, staging := newService(t)
		for attempt := range 2 {
			result, err := s.Put(context.Background(), bytes.NewReader(data))
			if err != nil {
				t.Fatal(err)
			}
			if result.ID != idFor(data) || result.Size != int64(len(data)) || result.Reused != (attempt == 1) {
				t.Fatalf("result %+v", result)
			}
			if !bytes.Equal(readContent(t, s, result.ID), data) {
				t.Fatal("content changed")
			}
			if ok, err := s.Exists(context.Background(), result.ID); !ok || err != nil {
				t.Fatal(ok, err)
			}
		}
		assertNoStaging(t, staging)
	}
}
func TestIDsAndMissing(t *testing.T) {
	s, _, _ := newService(t)
	for _, value := range []string{"", "sha256:", strings.Repeat("a", 64), "sha256:" + strings.Repeat("A", 64), "sha512:" + strings.Repeat("a", 64), "sha256:" + strings.Repeat("a", 63) + "/"} {
		if _, err := ParseID(value); !errors.Is(err, ErrInvalidID) {
			t.Fatal(value, err)
		}
		if _, err := s.Open(context.Background(), ID(value)); !errors.Is(err, ErrInvalidID) {
			t.Fatal(value, err)
		}
		if _, err := s.Exists(context.Background(), ID(value)); !errors.Is(err, ErrInvalidID) {
			t.Fatal(value, err)
		}
	}
	id := idFor([]byte("missing"))
	if _, err := ParseID(string(id)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Open(context.Background(), id); !errors.Is(err, storage.ErrNotFound) {
		t.Fatal(err)
	}
	if exists, err := s.Exists(context.Background(), id); exists || err != nil {
		t.Fatal(exists, err)
	}
}
func TestCorruptionNeverReused(t *testing.T) {
	s, blobs, staging := newService(t)
	data := []byte("expected")
	id := idFor(data)
	key, _ := storageKey(id)
	if err := blobs.Put(context.Background(), key, strings.NewReader("corrupt")); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.Exists(context.Background(), id); !ok || err != nil {
		t.Fatal(ok, err)
	}
	r, err := s.Open(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.ReadAll(r)
	if !errors.Is(err, ErrIntegrity) {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	result, err := s.Put(context.Background(), bytes.NewReader(data))
	var outcome *PutError
	if !errors.Is(err, ErrIntegrity) || !errors.As(err, &outcome) || outcome.Committed || result.Reused {
		t.Fatal(result, err)
	}
	assertNoStaging(t, staging)
}

type errorReader struct{ err error }

func (r errorReader) Read(p []byte) (int, error) { return copy(p, "partial"), r.err }
func TestSourceErrorsAndCancellation(t *testing.T) {
	s, _, staging := newService(t)
	failure := &PutError{Committed: true, Err: fs.ErrPermission}
	_, err := s.Put(context.Background(), errorReader{failure})
	var outcome *PutError
	if !errors.As(err, &outcome) || outcome.Committed || !errors.Is(err, failure) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Put(ctx, strings.NewReader("x")); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := s.Open(ctx, idFor(nil)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := s.Exists(ctx, idFor(nil)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	assertNoStaging(t, staging)
}

type stubStore struct {
	storage.Store
	put  func(context.Context, string, io.Reader) error
	open func(context.Context, string) (io.ReadCloser, error)
}

func (s stubStore) Put(ctx context.Context, key string, r io.Reader) error {
	if s.put != nil {
		return s.put(ctx, key, r)
	}
	return s.Store.Put(ctx, key, r)
}
func (s stubStore) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	if s.open != nil {
		return s.open(ctx, key)
	}
	return s.Store.Open(ctx, key)
}

func TestCommittedFailureAndStagingCleanup(t *testing.T) {
	original, blobs, staging := newService(t)
	failure := &fs.PathError{Op: "remove", Path: "temporary", Err: fs.ErrPermission}
	backend := stubStore{Store: blobs, put: func(ctx context.Context, key string, r io.Reader) error {
		if err := blobs.Put(ctx, key, r); err != nil {
			return err
		}
		return &storage.PutError{Published: true, Err: failure}
	}}
	s, err := New(backend, Options{TempDir: staging})
	if err != nil {
		t.Fatal(err)
	}
	result, err := s.Put(context.Background(), strings.NewReader("content"))
	var outcome *PutError
	if !errors.As(err, &outcome) || !outcome.Committed || !errors.Is(err, failure) || result.ID != idFor([]byte("content")) {
		t.Fatal(result, err)
	}
	if string(readContent(t, s, result.ID)) != "content" {
		t.Fatal("not published")
	}
	assertNoStaging(t, staging)
	var pending string
	result, err = original.put(context.Background(), strings.NewReader("second"), func(path string) error { pending = path; return failure })
	if !errors.As(err, &outcome) || !outcome.Committed || !errors.Is(err, failure) {
		t.Fatal(result, err)
	}
	if string(readContent(t, original, result.ID)) != "second" {
		t.Fatal("lost committed content")
	}
	if err := os.Remove(pending); err != nil {
		t.Fatal(err)
	}
	assertNoStaging(t, staging)
}
func TestDuplicateDoesNotHideAdditionalFailure(t *testing.T) {
	s, blobs, staging := newService(t)
	result, err := s.Put(context.Background(), strings.NewReader("content"))
	if err != nil {
		t.Fatal(err)
	}
	backend := stubStore{Store: blobs, put: func(context.Context, string, io.Reader) error {
		return &storage.PutError{Err: errors.Join(storage.ErrAlreadyExists, fs.ErrPermission)}
	}}
	s, err = New(backend, Options{TempDir: staging})
	if err != nil {
		t.Fatal(err)
	}
	reused, err := s.Put(context.Background(), strings.NewReader("content"))
	var outcome *PutError
	if !reused.Reused || reused.ID != result.ID || !errors.As(err, &outcome) || !outcome.Committed || !errors.Is(err, fs.ErrPermission) {
		t.Fatal(reused, err)
	}
	assertNoStaging(t, staging)
}
func TestConcurrentSameContent(t *testing.T) {
	s, _, staging := newService(t)
	var wg sync.WaitGroup
	results := make([]PutResult, 16)
	errs := make([]error, 16)
	for i := range results {
		wg.Go(func() { results[i], errs[i] = s.Put(context.Background(), strings.NewReader("concurrent")) })
	}
	wg.Wait()
	fresh := 0
	for i, result := range results {
		if errs[i] != nil {
			t.Fatal(errs[i])
		}
		if !result.Reused {
			fresh++
		}
		if result.ID != idFor([]byte("concurrent")) {
			t.Fatal(result)
		}
	}
	if fresh != 1 {
		t.Fatalf("fresh=%d", fresh)
	}
	assertNoStaging(t, staging)
}

type finalReader struct {
	data          []byte
	err, closeErr error
	closed        int
}

func (r *finalReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, r.err
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	if len(r.data) == 0 {
		return n, r.err
	}
	return n, nil
}
func (r *finalReader) Close() error { r.closed++; return r.closeErr }
func TestVerifiedReaderLifecycle(t *testing.T) {
	for _, tc := range []struct {
		name    string
		data    string
		readAll bool
		want    error
	}{{"verified final bytes", "right", true, nil}, {"corrupt final bytes", "wrong", true, ErrIntegrity}, {"early close", "wrong", false, nil}} {
		t.Run(tc.name, func(t *testing.T) {
			s, blobs, _ := newService(t)
			base := &finalReader{data: []byte(tc.data), err: io.EOF, closeErr: fs.ErrPermission}
			s.blobs = stubStore{Store: blobs, open: func(context.Context, string) (io.ReadCloser, error) { return base, nil }}
			c := new(observability.Collector)
			ctx := observability.With(context.Background(), observability.Options{Collector: c})
			r, err := s.Open(ctx, idFor([]byte("right")))
			if err != nil {
				t.Fatal(err)
			}
			if tc.readAll {
				_, err = io.ReadAll(r)
				if !errors.Is(err, tc.want) {
					t.Fatal(err)
				}
				_, again := r.Read(make([]byte, 10))
				if tc.want == nil && again != io.EOF {
					t.Fatal(again)
				}
				if tc.want != nil && !errors.Is(again, tc.want) {
					t.Fatal(again)
				}
			}
			for range 2 {
				if err := r.Close(); err != fs.ErrPermission {
					t.Fatal(err)
				}
			}
			if base.closed != 1 {
				t.Fatal("duplicate close")
			}
			if _, err := r.Read(make([]byte, 1)); err != io.ErrClosedPipe {
				t.Fatal(err)
			}
			m := c.Snapshot(observability.SnapshotOptions{})[observability.Scope{Layer: "cas", Operation: "open"}]
			if m.Completed != 1 || m.Errors != 1 {
				t.Fatal(m)
			}
			if (m.Counters["verified"] == 1) != (tc.name == "verified final bytes") {
				t.Fatal(m)
			}
		})
	}
}

type chunkSource struct {
	remaining int64
	maxRead   int
}

func (r *chunkSource) Read(p []byte) (int, error) {
	if len(p) > r.maxRead {
		r.maxRead = len(p)
	}
	if r.remaining == 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	for i := range p {
		p[i] = byte(i % 251)
	}
	r.remaining -= int64(len(p))
	return len(p), nil
}
func TestBoundedStreamingAndInternalObservation(t *testing.T) {
	s, _, staging := newService(t)
	c := new(observability.Collector)
	ctx := observability.WithInternal(observability.With(context.Background(), observability.Options{Collector: c, Observer: func(observability.Event) { t.Fatal("internal event leaked") }}))
	source := &chunkSource{remaining: 8 << 20}
	result, err := s.Put(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	if source.maxRead > 64<<10 || result.Size != 8<<20 {
		t.Fatal(source.maxRead, result)
	}
	if len(c.Snapshot(observability.SnapshotOptions{})) != 0 {
		t.Fatal("internal stats leaked")
	}
	m := c.Snapshot(observability.SnapshotOptions{IncludeInternal: true})[observability.Scope{Layer: "cas", Operation: "put"}]
	if m.Bytes != 8<<20 || m.Counters["published"] != 1 {
		t.Fatal(m)
	}
	assertNoStaging(t, staging)
}
func TestNewDoesNotCreateStaging(t *testing.T) {
	_, blobs, _ := newService(t)
	missing := filepath.Join(t.TempDir(), "missing")
	s, err := New(blobs, Options{TempDir: missing})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(missing); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal(err)
	}
	if _, err := s.Put(context.Background(), strings.NewReader("x")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal(err)
	}
	if _, err := New(nil, Options{}); err == nil {
		t.Fatal("nil backend accepted")
	}
}
