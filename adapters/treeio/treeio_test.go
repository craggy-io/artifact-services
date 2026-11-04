package treeio_test

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/craggy-io/artifact-services/adapters/testkit"
	"github.com/craggy-io/artifact-services/adapters/treeio"
	"github.com/craggy-io/artifact-services/artifacts"
	"github.com/craggy-io/artifact-services/lineage"
	"github.com/craggy-io/artifact-services/provenance"
)

func write(t *testing.T, root, path, data string) {
	t.Helper()
	full := filepath.Join(root, filepath.FromSlash(path))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
}

func services(s *testkit.Stack) treeio.Services {
	return treeio.Services{CAS: s.CAS, Artifacts: s.Artifacts, Lineage: s.Lineage}
}

func listing(t *testing.T, s *testkit.Stack, id artifacts.ID) map[string]artifacts.Entry {
	t.Helper()
	found := map[string]artifacts.Entry{}
	cursor := ""
	for {
		page, err := s.Artifacts.ListFiles(s.Ctx, id, artifacts.FileQuery{Expand: true, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range page.Entries {
			found[e.Path] = e
		}
		if cursor = page.NextCursor; cursor == "" {
			return found
		}
	}
}

func TestRoundTripPreservesTreeModesAndEmptyDirectories(t *testing.T) {
	s := testkit.Open(t)
	source := t.TempDir()
	write(t, source, "a.txt", "alpha")
	write(t, source, "dir/nested/b.txt", "beta")
	write(t, source, "same1", "identical")
	write(t, source, "same2", "identical")
	if err := os.MkdirAll(filepath.Join(source, "empty/deeper"), 0o755); err != nil {
		t.Fatal(err)
	}
	mode := func(path string, _ fs.FileInfo) uint32 {
		if path == "a.txt" {
			return 0o755
		}
		return 0o644
	}
	out, err := treeio.Ingest(s.Ctx, services(s), source, treeio.Options{Mode: mode, Name: "tree", Occurrence: "occ-tree"})
	if err != nil {
		t.Fatal(err)
	}
	if out.Files != 4 || out.Dirs != 1 || out.Bytes != int64(len("alpha")+len("beta")+2*len("identical")) {
		t.Fatalf("out = %+v", out)
	}
	entries := listing(t, s, out.Artifact)
	if entries["a.txt"].Mode != 0o755 || entries["empty/deeper"].Kind != artifacts.KindDir {
		t.Fatalf("entries = %+v", entries)
	}
	if _, nonEmptyListed := entries["dir"]; nonEmptyListed {
		t.Fatal("a directory with files should not need an entry")
	}
	if entries["same1"].Content != entries["same2"].Content {
		t.Fatal("identical files were not deduplicated")
	}

	dest := filepath.Join(t.TempDir(), "out")
	written, err := treeio.Materialize(s.Ctx, s.Artifacts, out.Artifact, dest, treeio.MaterializeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if written.Files != 4 || written.Dirs != 1 {
		t.Fatalf("written = %+v", written)
	}
	data, err := os.ReadFile(filepath.Join(dest, "dir", "nested", "b.txt"))
	if err != nil || string(data) != "beta" {
		t.Fatalf("b.txt = %q err = %v", data, err)
	}
	if info, err := os.Stat(filepath.Join(dest, "empty", "deeper")); err != nil || !info.IsDir() {
		t.Fatalf("empty dir = %v err = %v", info, err)
	}
	if runtime.GOOS != "windows" {
		if info, _ := os.Stat(filepath.Join(dest, "a.txt")); info.Mode().Perm() != 0o755 {
			t.Fatalf("mode = %v", info.Mode())
		}
	}
	if _, err := treeio.Materialize(s.Ctx, s.Artifacts, out.Artifact, dest, treeio.MaterializeOptions{}); !errors.Is(err, fs.ErrExist) {
		t.Fatalf("writing over an existing tree = %v", err)
	}
	if _, err := treeio.Materialize(s.Ctx, s.Artifacts, out.Artifact, dest, treeio.MaterializeOptions{Overwrite: true}); err != nil {
		t.Fatalf("overwrite = %v", err)
	}
	// Re-ingesting what came out gives the same artifact.
	again, err := treeio.Ingest(s.Ctx, services(s), dest, treeio.Options{Mode: mode})
	if err != nil || again.Artifact != out.Artifact {
		t.Fatalf("round trip = %s err = %v, want %s", again.Artifact, err, out.Artifact)
	}
}

func TestOriginIsRecordedAsAnExternalReference(t *testing.T) {
	s := testkit.Open(t)
	source := t.TempDir()
	write(t, source, "f", "x")
	out, err := treeio.Ingest(s.Ctx, services(s), source, treeio.Options{Occurrence: "o1", Name: "first"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(out.Source.URL, "file:///") || out.Source.Retrieval != string(out.Artifact) {
		t.Fatalf("source = %+v", out.Source)
	}
	occurrence, err := s.Artifacts.GetOccurrence(s.Ctx, "o1")
	if err != nil || occurrence.Origin != out.Source.URL {
		t.Fatalf("occurrence = %+v err = %v", occurrence, err)
	}
	page, err := s.Lineage.List(s.Ctx, lineage.Query{Ref: &out.Source})
	if err != nil || len(page.Records) != 1 || page.Records[0].Relationship.From.Occurrence != "o1" {
		t.Fatalf("origin = %+v err = %v", page, err)
	}
	// Idempotent under a stable occurrence ID.
	if _, err := treeio.Ingest(s.Ctx, services(s), source, treeio.Options{Occurrence: "o1", Name: "first"}); err != nil {
		t.Fatalf("retry = %v", err)
	}
	// A reader with a caller-supplied origin stands in for a download.
	download := provenance.Ref{URL: "https://example.test/x.zip", Retrieval: "etag-1"}
	got, err := treeio.IngestReader(s.Ctx, services(s), "x.zip", strings.NewReader("zipbytes"), 0o644, download, treeio.Options{Occurrence: "o2"})
	if err != nil || got.Source != download {
		t.Fatalf("reader = %+v err = %v", got, err)
	}
	if _, err := treeio.IngestReader(s.Ctx, services(s), "x", strings.NewReader("y"), 0, provenance.Ref{Digest: "sha256:abc"}, treeio.Options{}); err == nil {
		t.Fatal("non-external origin accepted")
	}
}

func TestSingleFileAndExclusions(t *testing.T) {
	s := testkit.Open(t)
	source := t.TempDir()
	write(t, source, "keep/a", "1")
	write(t, source, "skip/b", "2")
	write(t, source, "c.tmp", "3")
	exclude := func(path string, _ fs.DirEntry) bool { return path == "skip" || strings.HasSuffix(path, ".tmp") }
	out, err := treeio.Ingest(s.Ctx, services(s), source, treeio.Options{Exclude: exclude})
	if err != nil {
		t.Fatal(err)
	}
	entries := listing(t, s, out.Artifact)
	if len(entries) != 1 || entries["keep/a"].Size != 1 {
		t.Fatalf("entries = %+v", entries)
	}
	single, err := treeio.Ingest(s.Ctx, services(s), filepath.Join(source, "c.tmp"), treeio.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if entries := listing(t, s, single.Artifact); len(entries) != 1 || entries["c.tmp"].Size != 1 {
		t.Fatalf("single = %+v", entries)
	}
}

func requireSymlinks(t *testing.T, target, link string) {
	t.Helper()
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
}

func TestSymlinkPolicies(t *testing.T) {
	s := testkit.Open(t)
	source := t.TempDir()
	write(t, source, "real", "content")
	requireSymlinks(t, filepath.Join(source, "real"), filepath.Join(source, "link"))
	if _, err := treeio.Ingest(s.Ctx, services(s), source, treeio.Options{}); !errors.Is(err, treeio.ErrSymlink) {
		t.Fatalf("default = %v", err)
	}
	skipped, err := treeio.Ingest(s.Ctx, services(s), source, treeio.Options{Symlinks: treeio.SymlinkSkip})
	if err != nil {
		t.Fatal(err)
	}
	if entries := listing(t, s, skipped.Artifact); len(entries) != 1 {
		t.Fatalf("skip = %+v", entries)
	}
	followed, err := treeio.Ingest(s.Ctx, services(s), source, treeio.Options{Symlinks: treeio.SymlinkFollowInside})
	if err != nil {
		t.Fatal(err)
	}
	if entries := listing(t, s, followed.Artifact); entries["link"].Content != entries["real"].Content || entries["link"].Content == "" {
		t.Fatalf("follow = %+v", entries)
	}
	outside := filepath.Join(t.TempDir(), "secret")
	write(t, filepath.Dir(outside), "secret", "do not read")
	escaping := t.TempDir()
	requireSymlinks(t, outside, filepath.Join(escaping, "link"))
	if _, err := treeio.Ingest(s.Ctx, services(s), escaping, treeio.Options{Symlinks: treeio.SymlinkFollowInside}); !errors.Is(err, treeio.ErrSymlink) {
		t.Fatalf("escaping link = %v", err)
	}
}

func TestMaterializeRefusesToFollowLinksOutOfDestination(t *testing.T) {
	s := testkit.Open(t)
	id := s.File("sub/victim", "payload")
	dest := t.TempDir()
	outside := t.TempDir()
	requireSymlinks(t, outside, filepath.Join(dest, "sub"))
	if _, err := treeio.Materialize(s.Ctx, s.Artifacts, id, dest, treeio.MaterializeOptions{}); err == nil {
		t.Fatal("wrote through a link in the destination")
	}
	if left, _ := os.ReadDir(outside); len(left) != 0 {
		t.Fatalf("wrote outside the destination: %v", left)
	}
}

func TestMaterializeExpandsLinksAndDetectsCorruption(t *testing.T) {
	s := testkit.Open(t)
	inner := s.File("file", "embedded")
	content, size := s.Put("corruptible payload")
	outer, err := s.Artifacts.Publish(s.Ctx, artifacts.Manifest{Version: 1, Entries: []artifacts.Entry{
		{Path: "linked", Kind: artifacts.KindLink, Target: inner},
		{Path: "plain", Content: content, Size: size},
	}})
	if err != nil {
		t.Fatal(err)
	}
	dest := t.TempDir()
	if _, err := treeio.Materialize(s.Ctx, s.Artifacts, outer, dest, treeio.MaterializeOptions{}); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(filepath.Join(dest, "linked", "file")); err != nil || string(data) != "embedded" {
		t.Fatalf("embedded = %q err = %v", data, err)
	}
	key := strings.TrimPrefix(string(content), "sha256:")
	var blob string
	_ = filepath.WalkDir(filepath.Join(s.Root, "blobs"), func(path string, d os.DirEntry, _ error) error {
		if d != nil && !d.IsDir() && d.Name() == key {
			blob = path
		}
		return nil
	})
	raw, err := os.ReadFile(blob)
	if err != nil {
		t.Fatal(err)
	}
	raw[0] ^= 0xff
	if err := os.Chmod(blob, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(blob, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	second := t.TempDir()
	if _, err := treeio.Materialize(s.Ctx, s.Artifacts, outer, second, treeio.MaterializeOptions{}); err == nil {
		t.Fatal("corruption not detected")
	}
	if _, err := os.Stat(filepath.Join(second, "plain")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("corrupt file reached its final path: %v", err)
	}
	if left, _ := filepath.Glob(filepath.Join(second, "*.treeio-partial")); len(left) != 0 {
		t.Fatalf("partial left behind: %v", left)
	}
}

func TestLongPaths(t *testing.T) {
	s := testkit.Open(t)
	source := t.TempDir()
	deep := strings.Repeat("segment-of-a-long-path/", 14) + "file.txt"
	write(t, source, deep, "deep")
	out, err := treeio.Ingest(s.Ctx, services(s), source, treeio.Options{})
	if err != nil {
		t.Fatal(err)
	}
	dest := t.TempDir()
	if _, err := treeio.Materialize(s.Ctx, s.Artifacts, out.Artifact, dest, treeio.MaterializeOptions{}); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dest, filepath.FromSlash(deep)))
	if err != nil || string(data) != "deep" {
		t.Fatalf("deep = %q err = %v", data, err)
	}
}
