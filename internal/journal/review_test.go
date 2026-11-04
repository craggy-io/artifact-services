package journal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestReviewInvalidUTF8KeyRejected(t *testing.T) {
	root := t.TempDir()
	s, err := Open(root, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, _, err := s.Append(context.Background(), "bad\xffkey", []byte(`{"x":1}`)); err == nil {
		t.Fatal("invalid UTF-8 key accepted")
	}
}

func TestReviewCloseRetryAfterLeaseRecreated(t *testing.T) {
	root := t.TempDir()
	s, err := Open(root, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// Simulate a lease file that reappeared: a second Close must still
	// report success only because the first fully released it, and the
	// root must be reopenable.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, ".lease")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("lease remains: %v", err)
	}
	s2, err := Open(root, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
}
