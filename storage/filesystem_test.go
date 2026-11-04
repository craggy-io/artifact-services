package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func newTestStore(t *testing.T) (*Filesystem, string) {
	t.Helper()
	root := filepath.Join(t.TempDir(), "blobs")
	s, err := NewFilesystem(root)
	if err != nil {
		t.Fatal(err)
	}
	return s, root
}

func readBlob(t *testing.T, s Store, key string) []byte {
	t.Helper()
	r, err := s.Open(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(r)
	if closeErr := r.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func assertNoPending(t *testing.T, root string) {
	t.Helper()
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if strings.HasPrefix(entry.Name(), ".pending-") {
			t.Errorf("temporary file remains: %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRoundTrip(t *testing.T) {
	for _, value := range [][]byte{nil, {0, 1, 255}, bytes.Repeat([]byte("payload"), 10000)} {
		s, root := newTestStore(t)
		if err := s.Put(context.Background(), "abcdef", bytes.NewReader(value)); err != nil {
			t.Fatal(err)
		}
		reopened, err := NewFilesystem(root)
		if err != nil {
			t.Fatal(err)
		}
		if got := readBlob(t, reopened, "abcdef"); !bytes.Equal(got, value) {
			t.Fatal("roundtrip mismatch")
		}
		if exists, err := reopened.Exists(context.Background(), "abcdef"); err != nil || !exists {
			t.Fatalf("exists = %v, %v", exists, err)
		}
		if err := s.Put(context.Background(), "abcdef", strings.NewReader("replacement")); !errors.Is(err, ErrAlreadyExists) {
			t.Fatalf("duplicate: %v", err)
		}
		if got := readBlob(t, s, "abcdef"); !bytes.Equal(got, value) {
			t.Fatal("blob was overwritten")
		}
		assertNoPending(t, root)
	}
}

func TestMissingAndInvalidKeys(t *testing.T) {
	s, _ := newTestStore(t)
	if _, err := s.Open(context.Background(), "aa"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
	if exists, err := s.Exists(context.Background(), "aa"); exists || err != nil {
		t.Fatalf("missing exists: %v, %v", exists, err)
	}
	for _, key := range []string{"", "a", "../aa", "aa/../bb", `aa\bb`, "AA", "gg", "C:aa", "/aa", "aa\x00", "ab cd", "ab\n", "ab\t", "café", "ａｂ", "ab.", "ab:cd", `\\server\share`, strings.Repeat("a", 129)} {
		if err := s.Put(context.Background(), key, strings.NewReader("x")); !errors.Is(err, ErrInvalidKey) {
			t.Errorf("Put(%q): %v", key, err)
		}
		if _, err := s.Open(context.Background(), key); !errors.Is(err, ErrInvalidKey) {
			t.Errorf("Open(%q): %v", key, err)
		}
		if _, err := s.Exists(context.Background(), key); !errors.Is(err, ErrInvalidKey) {
			t.Errorf("Exists(%q): %v", key, err)
		}
	}
}

type failedReader struct{ err error }

func (r failedReader) Read(p []byte) (int, error) { return copy(p, "partial"), r.err }

func TestFailedWriteCleanup(t *testing.T) {
	s, root := newTestStore(t)
	failure := errors.New("source failure")
	if err := s.Put(context.Background(), "ab", failedReader{failure}); !errors.Is(err, failure) {
		t.Fatalf("source error lost: %v", err)
	}
	if exists, err := s.Exists(context.Background(), "ab"); exists || err != nil {
		t.Fatalf("partial published: %v, %v", exists, err)
	}
	assertNoPending(t, root)
	if err := os.Mkdir(filepath.Join(root, "ab", "ab"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(context.Background(), "ab", strings.NewReader("complete")); err == nil {
		t.Fatal("publication unexpectedly succeeded")
	}
	assertNoPending(t, root)
	if _, err := s.Open(context.Background(), "ab"); err == nil || errors.Is(err, ErrNotFound) {
		t.Fatalf("directory Open: %v", err)
	}
	if exists, err := s.Exists(context.Background(), "ab"); exists || err == nil {
		t.Fatalf("directory Exists: %v, %v", exists, err)
	}
}

func TestPendingWriteInvisible(t *testing.T) {
	s, root := newTestStore(t)
	r, w := io.Pipe()
	defer r.Close()
	defer w.Close()
	done := make(chan error, 1)
	go func() { done <- s.Put(context.Background(), "ab", r) }()
	if _, err := w.Write([]byte("first")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Open(context.Background(), "ab"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("pending Open: %v", err)
	}
	if exists, err := s.Exists(context.Background(), "ab"); exists || err != nil {
		t.Fatalf("pending Exists: %v, %v", exists, err)
	}
	if _, err := w.Write([]byte("second")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := string(readBlob(t, s, "ab")); got != "firstsecond" {
		t.Fatalf("got %q", got)
	}
	assertNoPending(t, root)
}

func TestConcurrentPublication(t *testing.T) {
	s, root := newTestStore(t)
	const writers = 16
	stores := make([]*Filesystem, writers)
	for i := range stores {
		var err error
		stores[i], err = NewFilesystem(root)
		if err != nil {
			t.Fatal(err)
		}
	}
	results := make([]error, writers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range writers {
		wg.Go(func() {
			<-start
			results[i] = stores[i].Put(context.Background(), "ff00", bytes.NewReader(bytes.Repeat([]byte{byte(i)}, 65536)))
		})
	}
	close(start)
	wg.Wait()
	winner := -1
	for i, err := range results {
		if err == nil {
			if winner != -1 {
				t.Fatal("multiple winners")
			}
			winner = i
		} else if !errors.Is(err, ErrAlreadyExists) {
			t.Fatalf("writer %d: %v", i, err)
		}
	}
	if winner == -1 {
		t.Fatal("no winner")
	}
	if got := readBlob(t, s, "ff00"); !bytes.Equal(got, bytes.Repeat([]byte{byte(winner)}, 65536)) {
		t.Fatal("incomplete or mixed content")
	}
	assertNoPending(t, root)
}

func TestRootAndFilesystemErrors(t *testing.T) {
	if _, err := NewFilesystem(""); err == nil {
		t.Fatal("empty root accepted")
	}
	s, root := newTestStore(t)
	if err := os.WriteFile(filepath.Join(root, "ab"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewFilesystem(filepath.Join(root, "ab")); err == nil {
		t.Fatal("file root accepted")
	}
	if err := s.Put(context.Background(), "abcd", strings.NewReader("x")); err == nil {
		t.Fatal("invalid shard accepted")
	}
	if err := s.Put(context.Background(), "ff", nil); err == nil {
		t.Fatal("nil source accepted")
	}
	assertNoPending(t, root)
}
