package gc_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/craggy-io/artifact-services/adapters/gc"
	"github.com/craggy-io/artifact-services/adapters/testkit"
	"github.com/craggy-io/artifact-services/artifacts"
	"github.com/craggy-io/artifact-services/assertions"
	"github.com/craggy-io/artifact-services/cas"
	"github.com/craggy-io/artifact-services/objects"
	"github.com/craggy-io/artifact-services/provenance"
)

func age(t *testing.T, s *testkit.Stack, id cas.ID, by time.Duration) {
	t.Helper()
	old := time.Now().Add(-by)
	if err := os.Chtimes(blobPath(t, s, id), old, old); err != nil {
		t.Fatal(err)
	}
}

func blobPath(t *testing.T, s *testkit.Stack, id cas.ID) string {
	t.Helper()
	key := strings.TrimPrefix(string(id), "sha256:")
	found := ""
	_ = filepath.WalkDir(filepath.Join(s.Root, "blobs"), func(path string, d os.DirEntry, _ error) error {
		if d != nil && !d.IsDir() && d.Name() == key {
			found = path
		}
		return nil
	})
	if found == "" {
		t.Fatalf("no blob %s", key)
	}
	return found
}

func contains(ids []cas.ID, id cas.ID) bool {
	for _, candidate := range ids {
		if candidate == id {
			return true
		}
	}
	return false
}

func TestScanAndSweepRemoveOnlyOldUnreachableContent(t *testing.T) {
	s := testkit.Open(t)
	s.File("kept", "reachable")
	old, _ := s.Put("old orphan")
	fresh, _ := s.Put("fresh orphan")
	age(t, s, old, 2*time.Hour)

	report, err := gc.Scan(s.Ctx, s.Blobs, s.Artifacts)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Orphans) != 2 || report.OrphanBytes == 0 || report.Stored != report.Reachable+2 || len(report.Missing) != 0 {
		t.Fatalf("report = %+v", report)
	}

	if _, err := gc.Sweep(s.Ctx, s.Blobs, s.Artifacts, gc.SweepOptions{}); !errors.Is(err, gc.ErrHorizon) {
		t.Fatalf("no horizon = %v", err)
	}

	dry, err := gc.Sweep(s.Ctx, s.Blobs, s.Artifacts, gc.SweepOptions{Horizon: time.Hour, DryRun: true})
	if err != nil || !contains(dry.Removed, old) {
		t.Fatalf("dry = %+v err = %v", dry, err)
	}
	if exists, _ := s.CAS.Exists(s.Ctx, old); !exists {
		t.Fatal("dry run removed content")
	}

	swept, err := gc.Sweep(s.Ctx, s.Blobs, s.Artifacts, gc.SweepOptions{Horizon: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if len(swept.Removed) != 1 || swept.Removed[0] != old || swept.Spared != 1 {
		t.Fatalf("swept = %+v", swept)
	}
	if exists, _ := s.CAS.Exists(s.Ctx, old); exists {
		t.Fatal("old orphan survived")
	}
	if exists, _ := s.CAS.Exists(s.Ctx, fresh); !exists {
		t.Fatal("the horizon did not protect fresh content")
	}
	check, err := gc.Verify(s.Ctx, gc.Services{CAS: s.CAS, Blobs: s.Blobs, Artifacts: s.Artifacts, Lineage: s.Lineage, Assertions: s.Assertions})
	if err != nil || !check.OK() {
		t.Fatalf("verify after sweep = %+v err = %v", check, err)
	}
}

func TestRePutRefreshesTheHorizon(t *testing.T) {
	s := testkit.Open(t)
	id, _ := s.Put("about to be reused")
	age(t, s, id, 2*time.Hour)
	s.Put("about to be reused")
	swept, err := gc.Sweep(s.Ctx, s.Blobs, s.Artifacts, gc.SweepOptions{Horizon: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if contains(swept.Removed, id) {
		t.Fatal("re-Put content was swept")
	}
}

func TestPublicationAfterASweepFailsCleanlyNeverDangling(t *testing.T) {
	s := testkit.Open(t)
	id, size := s.Put("staged then swept")
	age(t, s, id, 2*time.Hour)
	if _, err := gc.Sweep(s.Ctx, s.Blobs, s.Artifacts, gc.SweepOptions{Horizon: time.Hour}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Artifacts.Publish(s.Ctx, artifacts.Manifest{Version: 1, Entries: []artifacts.Entry{{Path: "f", Content: id, Size: size}}}); err == nil {
		t.Fatal("published an artifact whose content is gone")
	}
	page, err := s.Artifacts.Search(s.Ctx, artifacts.CatalogQuery{})
	if err != nil || len(page.Items) != 0 {
		t.Fatalf("dangling artifact registered: %+v err = %v", page, err)
	}
}

func TestFenceBlocksPublicationDuringSweepWindow(t *testing.T) {
	s := testkit.Open(t)
	id, size := s.Put("x")
	entered, release, published := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	fenced := make(chan error, 1)
	go func() {
		fenced <- s.Artifacts.Fence(s.Ctx, func(context.Context) error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered
	go func() {
		_, err := s.Artifacts.Publish(s.Ctx, artifacts.Manifest{Version: 1, Entries: []artifacts.Entry{{Path: "f", Content: id, Size: size}}})
		published <- err
	}()
	select {
	case err := <-published:
		t.Fatalf("publication ran inside the fence: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	if err := <-fenced; err != nil {
		t.Fatal(err)
	}
	if err := <-published; err != nil {
		t.Fatal(err)
	}
}

func TestVerifyFindsCorruptionAndMissingContent(t *testing.T) {
	s := testkit.Open(t)
	kind, err := s.Assertions.Register(s.Ctx, assertions.Kind{Name: "note", Version: 1, Shape: objects.Shape{Type: "integer"}}, assertions.Writer{})
	if err != nil {
		t.Fatal(err)
	}
	content, size := s.Put("verified content")
	id, err := s.Artifacts.Publish(s.Ctx, artifacts.Manifest{Version: 1, Entries: []artifacts.Entry{{Path: "f", Content: content, Size: size}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Artifacts.RecordOccurrence(s.Ctx, artifacts.OccurrenceRequest{ID: "o", Artifact: id}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Assertions.Record(s.Ctx, assertions.Request{ID: "a", Kind: kind, Subject: provenance.Ref{Occurrence: "o"}, Value: json.RawMessage("1")}); err != nil {
		t.Fatal(err)
	}
	services := gc.Services{CAS: s.CAS, Blobs: s.Blobs, Artifacts: s.Artifacts, Lineage: s.Lineage, Assertions: s.Assertions}
	clean, err := gc.Verify(s.Ctx, services)
	if err != nil || !clean.OK() || clean.ContentChecked == 0 || clean.RecordsChecked == 0 {
		t.Fatalf("clean = %+v err = %v", clean, err)
	}
	path := blobPath(t, s, content)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[0] ^= 0xff
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	dirty, err := gc.Verify(s.Ctx, services)
	if err != nil || dirty.OK() {
		t.Fatalf("dirty = %+v err = %v", dirty, err)
	}
	found := false
	for _, f := range dirty.Findings {
		found = found || f.Layer == "cas" && f.ID == string(content)
	}
	if !found {
		t.Fatalf("corrupted blob not named: %+v", dirty.Findings)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	report, err := gc.Scan(s.Ctx, s.Blobs, s.Artifacts)
	if err != nil || !contains(report.Missing, content) {
		t.Fatalf("missing content not reported: %+v err = %v", report, err)
	}
}
