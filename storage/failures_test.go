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
	"testing"
)

func assertRoundTrip(t *testing.T, s Store, key string) {
	t.Helper()
	value := []byte("binary\x00payload\xff")
	if err := s.Put(context.Background(), key, bytes.NewReader(value)); err != nil {
		t.Fatal(err)
	}
	if got := readBlob(t, s, key); !bytes.Equal(got, value) {
		t.Fatalf("wrong content: %q", got)
	}
	if exists, err := s.Exists(context.Background(), key); !exists || err != nil {
		t.Fatalf("Exists: %v, %v", exists, err)
	}
}

func TestKeyBoundariesAndRootNames(t *testing.T) {
	root := filepath.Join(t.TempDir(), "spaces and 日本語 café")
	s, err := NewFilesystem(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"00", "abc", strings.Repeat("f", 128)} {
		assertRoundTrip(t, s, key)
	}
	assertNoPending(t, root)
}

func TestInvalidRootNUL(t *testing.T) {
	_, err := NewFilesystem(filepath.Join(t.TempDir(), "bad\x00root"))
	if err == nil {
		t.Fatal("NUL root accepted")
	}
}

func TestMissingRootRecreatedByPut(t *testing.T) {
	s, root := newTestStore(t)
	// Remove only this empty fixture directory.
	if err := os.Remove(root); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Open(context.Background(), "ab"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Open: %v", err)
	}
	if exists, err := s.Exists(context.Background(), "ab"); exists || err != nil {
		t.Fatalf("Exists: %v, %v", exists, err)
	}
	assertRoundTrip(t, s, "ab")
	assertNoPending(t, root)
}

func TestClosedSourceAndFailedReplacement(t *testing.T) {
	s, root := newTestStore(t)
	f, err := os.CreateTemp(t.TempDir(), "source")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(context.Background(), "ab", f); !errors.Is(err, fs.ErrClosed) {
		t.Fatalf("closed source error: %v", err)
	}
	if exists, err := s.Exists(context.Background(), "ab"); exists || err != nil {
		t.Fatalf("failed write published: %v, %v", exists, err)
	}
	assertRoundTrip(t, s, "ab")
	original := readBlob(t, s, "ab")
	failure := errors.New("input interrupted")
	if err := s.Put(context.Background(), "ab", failedReader{failure}); !errors.Is(err, failure) {
		t.Fatalf("source error lost: %v", err)
	}
	if got := readBlob(t, s, "ab"); !bytes.Equal(got, original) {
		t.Fatal("failed replacement changed blob")
	}
	assertNoPending(t, root)
}

func TestInterruptedStreamCleanup(t *testing.T) {
	s, root := newTestStore(t)
	r, w := io.Pipe()
	defer r.Close()
	defer w.Close()
	done := make(chan error, 1)
	go func() { done <- s.Put(context.Background(), "ab", r) }()
	if _, err := w.Write(bytes.Repeat([]byte("partial"), 10000)); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("stream interrupted")
	if err := w.CloseWithError(failure); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, failure) {
		t.Fatalf("stream error lost: %v", err)
	}
	if exists, err := s.Exists(context.Background(), "ab"); exists || err != nil {
		t.Fatalf("partial published: %v, %v", exists, err)
	}
	assertNoPending(t, root)
	assertRoundTrip(t, s, "ab")
}

func TestSentinelsPreserveUnderlyingErrors(t *testing.T) {
	s, _ := newTestStore(t)
	_, err := s.Open(context.Background(), "ab")
	var pathErr *fs.PathError
	if !errors.Is(err, ErrNotFound) || !errors.Is(err, fs.ErrNotExist) || !errors.As(err, &pathErr) {
		t.Errorf("missing OS error lost: %v", err)
	}
	assertRoundTrip(t, s, "ab")
	err = s.Put(context.Background(), "ab", strings.NewReader("duplicate"))
	var linkErr *os.LinkError
	if !errors.Is(err, ErrAlreadyExists) || !errors.Is(err, fs.ErrExist) || !errors.As(err, &linkErr) {
		t.Errorf("duplicate OS error lost: %v", err)
	}
}
