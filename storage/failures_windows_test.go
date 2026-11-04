package storage

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strings"
	"testing"
)

func TestWindowsLongPaths(t *testing.T) {
	root := t.TempDir()
	for len(root) < 320 {
		root = filepath.Join(root, "long-directory-segment")
	}
	s, err := NewFilesystem(root)
	if err != nil {
		t.Fatal(err)
	}
	key := strings.Repeat("a", 128)
	t.Logf("root length=%d, blob path length=%d", len(root), len(filepath.Join(root, key[:2], key)))
	assertRoundTrip(t, s, key)
	assertNoPending(t, root)
}

func TestWindowsInvalidRootCharacters(t *testing.T) {
	for _, name := range []string{"bad<root", "bad>root", "bad|root", "bad?root", "bad*root", "bad\"root"} {
		t.Run(name, func(t *testing.T) {
			_, err := NewFilesystem(filepath.Join(t.TempDir(), name))
			var pathErr *fs.PathError
			if !errors.As(err, &pathErr) {
				t.Fatalf("expected OS rejection, got %v", err)
			}
		})
	}
}

// Restrict only disposable fixtures, and restore the ACL before TempDir cleanup.
func denyFixtureAccess(t *testing.T, path, rights string) {
	t.Helper()
	tool, err := exec.LookPath("icacls.exe")
	if err != nil {
		t.Skip("icacls unavailable; cannot exercise actual Windows ACL denial")
	}
	current, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	sid := "*" + current.Uid
	run := func(args ...string) {
		t.Helper()
		if out, err := exec.Command(tool, args...).CombinedOutput(); err != nil {
			t.Fatalf("icacls: %v: %s", err, out)
		}
	}
	t.Cleanup(func() { run(path, "/remove:d", sid, "/q") })
	run(path, "/deny", sid+":("+rights+")", "/q")
}

func TestWindowsPermissionDenied(t *testing.T) {
	t.Run("create temporary file", func(t *testing.T) {
		s, root := newTestStore(t)
		shard := filepath.Join(root, "ab")
		if err := os.Mkdir(shard, 0700); err != nil {
			t.Fatal(err)
		}
		denyFixtureAccess(t, shard, "WD")
		err := s.Put(context.Background(), "abcd", strings.NewReader("data"))
		var pathErr *fs.PathError
		if !errors.Is(err, fs.ErrPermission) || !errors.As(err, &pathErr) {
			t.Fatalf("expected OS permission error, got %v", err)
		}
		if exists, err := s.Exists(context.Background(), "abcd"); exists || err != nil {
			t.Fatalf("failed write published: %v, %v", exists, err)
		}
		assertNoPending(t, root)
	})
	t.Run("open blob", func(t *testing.T) {
		s, root := newTestStore(t)
		assertRoundTrip(t, s, "abcd")
		denyFixtureAccess(t, filepath.Join(root, "ab", "abcd"), "RD")
		r, err := s.Open(context.Background(), "abcd")
		if r != nil {
			r.Close()
		}
		var pathErr *fs.PathError
		if !errors.Is(err, fs.ErrPermission) || !errors.As(err, &pathErr) {
			t.Fatalf("expected OS permission error, got %v", err)
		}
	})
}
