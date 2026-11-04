package spool_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/craggy-io/artifact-services/adapters/spool"
	"github.com/craggy-io/artifact-services/adapters/testkit"
	"github.com/craggy-io/artifact-services/artifacts"
	"github.com/craggy-io/artifact-services/cas"
)

func blobPath(t *testing.T, root string, id cas.ID) string {
	t.Helper()
	key := strings.TrimPrefix(string(id), "sha256:")
	var found string
	err := filepath.WalkDir(filepath.Join(root, "blobs"), func(path string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && d.Name() == key {
			found = path
		}
		return err
	})
	if err != nil || found == "" {
		t.Fatalf("blob %s: found %q err %v", key, found, err)
	}
	return found
}

func TestCopyReturnsVerifiedRandomAccessAndCleansUp(t *testing.T) {
	s := testkit.Open(t)
	id := s.File("f", "0123456789")
	reader, err := s.Artifacts.OpenFile(s.Ctx, id, "f")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	file, err := spool.Copy(s.Ctx, reader, dir)
	if err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 3)
	if _, err := file.ReadAt(buffer, 4); err != nil || string(buffer) != "456" {
		t.Fatalf("read at = %q err = %v", buffer, err)
	}
	if _, err := file.Seek(8, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	rest, err := io.ReadAll(file)
	if err != nil || string(rest) != "89" {
		t.Fatalf("rest = %q err = %v", rest, err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if left, _ := os.ReadDir(dir); len(left) != 0 {
		t.Fatalf("spool left %v", left)
	}
}

// TestCorruptionIsDetectedOnEveryPath is qualification scenario 10.
func TestCorruptionIsDetectedOnEveryPath(t *testing.T) {
	s := testkit.Open(t)
	const data = "the quick brown fox jumps over the lazy dog"
	content, size := s.Put(data)
	id, err := s.Artifacts.Publish(s.Ctx, artifacts.Manifest{Version: 1, Entries: []artifacts.Entry{{Path: "f", Content: content, Size: size}}})
	if err != nil {
		t.Fatal(err)
	}

	// Intact content passes everywhere first, so a failure below means detection.
	seekable, err := s.Artifacts.OpenSeekable(s.Ctx, id, "f")
	if err != nil {
		t.Fatal(err)
	}
	seekable.Close()

	path := blobPath(t, s.Root, content)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)/2] ^= 0xff
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	// Streaming: detected at EOF, after bytes were delivered.
	reader, err := s.Artifacts.OpenFile(s.Ctx, id, "f")
	if err != nil {
		t.Fatal(err)
	}
	_, streamErr := io.ReadAll(reader)
	reader.Close()
	if !errors.Is(streamErr, cas.ErrIntegrity) {
		t.Errorf("streaming read = %v, want ErrIntegrity", streamErr)
	}

	// Seekable: detected on open, before any bytes are handed over.
	if file, err := s.Artifacts.OpenSeekable(s.Ctx, id, "f"); !errors.Is(err, cas.ErrIntegrity) {
		if file != nil {
			file.Close()
		}
		t.Errorf("seekable open = %v, want ErrIntegrity", err)
	}

	// Spool: detected before the copy is returned, and the copy does not survive.
	reader, err = s.Artifacts.OpenFile(s.Ctx, id, "f")
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if file, err := spool.Copy(s.Ctx, reader, dir); !errors.Is(err, cas.ErrIntegrity) {
		if file != nil {
			file.Close()
		}
		t.Errorf("spool = %v, want ErrIntegrity", err)
	}
	if left, _ := os.ReadDir(dir); len(left) != 0 {
		t.Errorf("failed spool left %v", left)
	}

	if file, err := spool.OpenFile(s.Ctx, s.Artifacts, id, "f", dir); !errors.Is(err, cas.ErrIntegrity) {
		if file != nil {
			file.Close()
		}
		t.Errorf("spool.OpenFile = %v, want ErrIntegrity", err)
	}
}

func TestCopyHonorsCancellation(t *testing.T) {
	s := testkit.Open(t)
	id := s.File("f", "abc")
	reader, err := s.Artifacts.OpenFile(s.Ctx, id, "f")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(s.Ctx)
	cancel()
	dir := t.TempDir()
	if _, err := spool.Copy(ctx, reader, dir); !errors.Is(err, context.Canceled) {
		t.Fatalf("copy = %v", err)
	}
	if left, _ := os.ReadDir(dir); len(left) != 0 {
		t.Fatalf("left %v", left)
	}
}
