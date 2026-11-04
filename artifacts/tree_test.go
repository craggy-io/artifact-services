package artifacts

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/craggy-io/artifact-services/cas"
	"github.com/craggy-io/artifact-services/observability"
	"github.com/craggy-io/artifact-services/provenance"
)

func listAll(t *testing.T, s *Service, id ID, q FileQuery) []Entry {
	t.Helper()
	var all []Entry
	for {
		page, err := s.ListFiles(context.Background(), id, q)
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, page.Entries...)
		if page.NextCursor == "" {
			return all
		}
		q.Cursor = page.NextCursor
	}
}

func heap() uint64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// entryCount picks scale. The full figure runs only with the opt-in trial switch,
// like the other large trials in docs/verification.md.
func entryCount(t *testing.T, full, quick int) int {
	t.Helper()
	if os.Getenv("ARTIFACT_SERVICES_LARGE_TRIALS") != "" {
		return full
	}
	return quick
}

// TestBuilderPublishesLargeTreesWithBoundedHeap is qualification scenario 1. The
// builder streams, so heap stays flat however many entries the tree has.
func TestBuilderPublishesLargeTreesWithBoundedHeap(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	total := entryCount(t, 1_000_000, 50_000)
	put, err := f.content.Put(ctx, strings.NewReader("shared"))
	if err != nil {
		t.Fatal(err)
	}
	before := heap()
	builder := f.service.NewBuilder(BuilderOptions{MaxMemory: 8 << 20})
	defer builder.Close()
	// Out of order on purpose: identity must not depend on arrival order.
	rng := rand.New(rand.NewSource(7))
	for _, i := range rng.Perm(total) {
		path := fmt.Sprintf("decompiled/pkg%03d/File%07d.java", i%997, i)
		if err := builder.Add(Entry{Path: path, Content: put.ID, Size: put.Size, Mode: 0o644}); err != nil {
			t.Fatal(err)
		}
	}
	id, err := builder.Publish(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.Close(); err != nil {
		t.Fatal(err)
	}
	if grew := int64(heap()) - int64(before); grew > 64<<20 {
		t.Fatalf("publishing %d entries grew heap by %d bytes", total, grew)
	}
	item, err := f.service.Search(ctx, CatalogQuery{ID: id})
	if err != nil || len(item.Items) != 1 || item.Items[0].FileCount != int64(total) {
		t.Fatal(item, err)
	}
	// Paging reaches every entry in stable order with bounded per-page work.
	count, previous := 0, ""
	cursor := ""
	for {
		page, err := f.service.ListFiles(ctx, id, FileQuery{Limit: 1000, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range page.Entries {
			if previous != "" && comparePaths(previous, e.Path) >= 0 {
				t.Fatalf("out of order %q then %q", previous, e.Path)
			}
			previous = e.Path
			count++
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	if count != total {
		t.Fatalf("listed %d of %d", count, total)
	}
	if grew := int64(heap()) - int64(before); grew > 64<<20 {
		t.Fatalf("paging grew heap by %d bytes", grew)
	}
	if got := readFile(t, f.service, id, fmt.Sprintf("decompiled/pkg%03d/File%07d.java", 5%997, 5)); got != "shared" {
		t.Fatal(got)
	}
	if _, err := f.service.OpenFile(ctx, id, "decompiled/absent"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	// A whole-list read of an oversized manifest is refused, not attempted.
	if total > f.service.options.MaxMaterializedEntries {
		if _, err := f.service.Get(ctx, id); !errors.Is(err, ErrLimit) {
			t.Fatal(err)
		}
	}
	// Identity round-trips: building the same entries in another order is the same artifact.
	again := f.service.NewBuilder(BuilderOptions{MaxMemory: 8 << 20})
	defer again.Close()
	for i := total - 1; i >= 0; i-- {
		path := fmt.Sprintf("decompiled/pkg%03d/File%07d.java", i%997, i)
		if err := again.Add(Entry{Path: path, Content: put.ID, Size: put.Size, Mode: 0o644}); err != nil {
			t.Fatal(err)
		}
	}
	second, err := again.Publish(ctx)
	if err != nil || second != id {
		t.Fatal(second, id, err)
	}
}

func TestSinglePathReadsOneBlock(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	put, err := f.content.Put(ctx, strings.NewReader("x"))
	if err != nil {
		t.Fatal(err)
	}
	counting := &countingCAS{base: f.content}
	service := f.service
	service.blobs = counting
	builder := service.NewBuilder(BuilderOptions{})
	defer builder.Close()
	const total = 3 * maxBlockEntries
	for i := 0; i < total; i++ {
		if err := builder.Add(Entry{Path: fmt.Sprintf("f%06d", i), Content: put.ID, Size: 1}); err != nil {
			t.Fatal(err)
		}
	}
	id, err := builder.Publish(ctx)
	if err != nil {
		t.Fatal(err)
	}
	service.cache = blockCache{}
	counting.opens.Store(0)
	if _, err := service.Stat(ctx, id, "f001234"); err != nil {
		t.Fatal(err)
	}
	if got := counting.opens.Load(); got != 2 {
		t.Fatalf("stat opened %d objects, want the root and one block", got)
	}
}

func TestEntryKindsModesAndEmbeddedArtifacts(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	file := func(path, content string, mode uint32) Entry {
		put, err := f.content.Put(ctx, strings.NewReader(content))
		if err != nil {
			t.Fatal(err)
		}
		return Entry{Path: path, Content: put.ID, Size: put.Size, Mode: mode}
	}
	lib := publish(t, f.service, ctx, Manifest{Entries: []Entry{
		file("bin/tool", "#!tool", 0o755),
		file("doc/readme", "docs", 0o644),
		{Path: "empty", Kind: KindDir, Mode: 0o700},
	}})
	entries := []Entry{
		file("run.sh", "#!/bin/sh", 0o755),
		{Path: "logs", Kind: KindDir, Mode: 0o750},
		{Path: "vendor", Kind: KindLink, Target: lib},
		{Path: "docs", Kind: KindLink, Target: lib, TargetPath: "doc"},
		{Path: "readme", Kind: KindLink, Target: lib, TargetPath: "doc/readme"},
		file("zz", "last", 0),
	}
	id := publish(t, f.service, ctx, Manifest{Entries: entries})

	got := listAll(t, f.service, id, FileQuery{})
	if len(got) != len(entries) {
		t.Fatal(got)
	}
	byPath := map[string]Entry{}
	for _, e := range got {
		byPath[e.Path] = e
	}
	if byPath["run.sh"].Mode != 0o755 || byPath["logs"].Kind != KindDir || byPath["logs"].Mode != 0o750 || byPath["vendor"].Target != lib {
		t.Fatal(byPath)
	}

	// Expansion embeds without copying, and keeps modes and bytes exactly.
	expanded := map[string]Entry{}
	for _, e := range listAll(t, f.service, id, FileQuery{Expand: true}) {
		expanded[e.Path] = e
	}
	if len(expanded) != 8 {
		t.Fatalf("expanded %v", expanded)
	}
	if expanded["vendor/bin/tool"].Mode != 0o755 || expanded["vendor/empty"].Kind != KindDir || expanded["docs/readme"].Size != 4 || expanded["readme"].Mode != 0o644 {
		t.Fatal(expanded)
	}
	for path, want := range map[string]string{"vendor/bin/tool": "#!tool", "docs/readme": "docs", "readme": "docs", "run.sh": "#!/bin/sh"} {
		if got := readFile(t, f.service, id, path); got != want {
			t.Fatalf("%s: %q", path, got)
		}
	}
	for _, path := range []string{"logs", "vendor/empty"} {
		if _, err := f.service.OpenFile(ctx, id, path); !errors.Is(err, ErrNotFile) {
			t.Fatalf("%s: %v", path, err)
		}
	}
	if _, err := f.service.OpenFile(ctx, id, "vendor/absent"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	info, err := f.service.Stat(ctx, id, "vendor/bin/tool")
	if err != nil || info.Mode != 0o755 || info.Size != 6 || info.Path != "vendor/bin/tool" {
		t.Fatal(info, err)
	}

	// A prefix reaches inside a link, and paging resumes from inside a link.
	prefixed := listAll(t, f.service, id, FileQuery{Prefix: "vendor/bin", Expand: true})
	if len(prefixed) != 1 || prefixed[0].Path != "vendor/bin/tool" {
		t.Fatal(prefixed)
	}
	paged := listAll(t, f.service, id, FileQuery{Expand: true, Limit: 1})
	if len(paged) != 8 {
		t.Fatalf("one-at-a-time expansion: %v", paged)
	}
	seen := map[string]bool{}
	for _, e := range paged {
		if seen[e.Path] {
			t.Fatalf("duplicate %s", e.Path)
		}
		seen[e.Path] = true
	}

	// Equal content with a different mode is a different snapshot, but the same file.
	a := publish(t, f.service, ctx, Manifest{Entries: []Entry{file("x", "same", 0o644)}})
	b := publish(t, f.service, ctx, Manifest{Entries: []Entry{file("x", "same", 0o755)}})
	if a == b {
		t.Fatal("mode did not reach identity")
	}

	// Reopen: nothing here depends on resident state.
	if err := f.service.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := New(f.content, Options{Root: f.root})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if err := reopened.Rebuild(ctx); err != nil {
		t.Fatal(err)
	}
	if got := readFile(t, reopened, id, "vendor/bin/tool"); got != "#!tool" {
		t.Fatal(got)
	}
}

func TestInvalidEntriesAreRejected(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	put, err := f.content.Put(ctx, strings.NewReader("x"))
	if err != nil {
		t.Fatal(err)
	}
	file := Entry{Path: "a", Content: put.ID, Size: 1}
	unregistered := ID(put.ID)
	cases := map[string][]Entry{
		"file with no content":     {{Path: "a"}},
		"bad mode":                 {{Path: "a", Content: put.ID, Size: 1, Mode: 0o10000}},
		"unknown kind":             {{Path: "a", Kind: "socket"}},
		"directory with content":   {{Path: "a", Kind: KindDir, Content: put.ID}},
		"directory with size":      {{Path: "a", Kind: KindDir, Size: 1}},
		"link with no target":      {{Path: "a", Kind: KindLink}},
		"link with content":        {{Path: "a", Kind: KindLink, Target: unregistered, Content: put.ID}},
		"link to unregistered":     {{Path: "a", Kind: KindLink, Target: unregistered}},
		"file with target":         {{Path: "a", Content: put.ID, Size: 1, Target: unregistered}},
		"nested under file":        {file, {Path: "a/b", Content: put.ID, Size: 1}},
		"nested under link":        {{Path: "a", Kind: KindLink, Target: unregistered}, {Path: "a/b", Content: put.ID, Size: 1}},
		"duplicate":                {file, file},
		"bad target path":          {{Path: "a", Kind: KindLink, Target: unregistered, TargetPath: "../x"}},
		"parent directory segment": {{Path: "a/../b", Content: put.ID, Size: 1}},
	}
	for name, entries := range cases {
		if _, err := f.service.Publish(ctx, Manifest{Entries: entries}); !errors.Is(err, ErrInvalidManifest) {
			t.Fatalf("%s: %v", name, err)
		}
	}
	real := publish(t, f.service, ctx, Manifest{Entries: []Entry{file}})
	if _, err := f.service.Publish(ctx, Manifest{Entries: []Entry{{Path: "l", Kind: KindLink, Target: real, TargetPath: "absent"}}}); !errors.Is(err, ErrInvalidManifest) {
		t.Fatal(err)
	}
	// A directory may hold entries, and the conflict check is not fooled by a sibling
	// that sorts between a file and where its descendants would be.
	publish(t, f.service, ctx, Manifest{Entries: []Entry{{Path: "a", Kind: KindDir}, {Path: "a/b", Content: put.ID, Size: 1}, {Path: "a-b", Content: put.ID, Size: 1}}})
}

func TestLinkDepthIsBounded(t *testing.T) {
	ctx := context.Background()
	base := newFixture(t)
	if err := base.service.Close(); err != nil {
		t.Fatal(err)
	}
	service, err := New(base.content, Options{Root: base.root, MaxLinkDepth: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	put, _ := base.content.Put(ctx, strings.NewReader("x"))
	id := publish(t, service, ctx, Manifest{Entries: []Entry{{Path: "f", Content: put.ID, Size: 1}}})
	id = publish(t, service, ctx, Manifest{Entries: []Entry{{Path: "one", Kind: KindLink, Target: id}}})
	id = publish(t, service, ctx, Manifest{Entries: []Entry{{Path: "two", Kind: KindLink, Target: id}}})
	if got := readFile(t, service, id, "two/one/f"); got != "x" {
		t.Fatal(got)
	}
	if _, err := service.Publish(ctx, Manifest{Entries: []Entry{{Path: "three", Kind: KindLink, Target: id}}}); !errors.Is(err, ErrInvalidManifest) {
		t.Fatal(err)
	}
}

func TestOccurrencesKeepIdenticalContentSeparate(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, "lab")
	manifest := f.manifest(t, "app.apk", "same bytes")
	id := publish(t, f.service, ctx, manifest)
	one, err := f.service.RecordOccurrence(ctx, OccurrenceRequest{ID: "import-1", Artifact: id, Name: "first", Origin: "file:///a", Producer: "run-1", Metadata: map[string]json.RawMessage{"lab": json.RawMessage(`"x"`)}})
	if err != nil {
		t.Fatal(err)
	}
	two, err := f.service.RecordOccurrence(ctx, OccurrenceRequest{Artifact: id, Name: "second", Origin: "file:///b"})
	if err != nil {
		t.Fatal(err)
	}
	if one.ID == two.ID || one.Artifact != two.Artifact || one.Content != two.Content || two.ID == "" {
		t.Fatal(one, two)
	}
	// The same bytes under another file name are a different snapshot, same content.
	put, _ := f.content.Put(ctx, strings.NewReader("same bytes"))
	renamed := publish(t, f.service, ctx, Manifest{Entries: []Entry{{Path: "renamed.apk", Content: put.ID, Size: put.Size, Mode: 0o755}}})
	if renamed == id {
		t.Fatal("different manifest, same artifact")
	}
	three, err := f.service.RecordOccurrence(ctx, OccurrenceRequest{ID: "import-3", Artifact: renamed, Name: "third"})
	if err != nil {
		t.Fatal(err)
	}
	if three.Content != one.Content || three.Content != put.ID {
		t.Fatal(three.Content, one.Content)
	}

	// One call each: get one, find all by digest, find by snapshot, by name, by metadata.
	got, err := f.service.GetOccurrence(ctx, "import-1")
	if err != nil || got.Name != "first" || got.Origin != "file:///a" || got.Producer != "run-1" || got.Sequence != one.Sequence {
		t.Fatal(got, err)
	}
	all := func(q OccurrenceQuery) []OccurrenceID {
		t.Helper()
		page, err := f.service.Occurrences(ctx, q)
		if err != nil {
			t.Fatal(err)
		}
		ids := []OccurrenceID{}
		for _, o := range page.Occurrences {
			ids = append(ids, o.ID)
		}
		return ids
	}
	if ids := all(OccurrenceQuery{Content: one.Content}); !reflect.DeepEqual(ids, []OccurrenceID{"import-1", two.ID, "import-3"}) {
		t.Fatal(ids)
	}
	if ids := all(OccurrenceQuery{Artifact: id}); len(ids) != 2 {
		t.Fatal(ids)
	}
	if ids := all(OccurrenceQuery{Name: "second"}); len(ids) != 1 || ids[0] != two.ID {
		t.Fatal(ids)
	}
	if ids := all(OccurrenceQuery{Metadata: map[string]json.RawMessage{"lab": json.RawMessage(`"x"`)}}); len(ids) != 1 || ids[0] != "import-1" {
		t.Fatal(ids)
	}
	if _, err := f.service.Occurrences(ctx, OccurrenceQuery{}); !errors.Is(err, ErrInvalidQuery) {
		t.Fatal(err)
	}
	if _, err := f.service.Occurrences(ctx, OccurrenceQuery{Metadata: map[string]json.RawMessage{"unindexed": json.RawMessage(`1`)}}); !errors.Is(err, ErrInvalidQuery) {
		t.Fatal(err)
	}
	// Catalog search also reaches content.
	page, err := f.service.Search(ctx, CatalogQuery{Content: one.Content})
	if err != nil || len(page.Items) != 2 {
		t.Fatal(page, err)
	}

	// Idempotent retry with the same ID; a different body under it is a conflict.
	again, err := f.service.RecordOccurrence(ctx, OccurrenceRequest{ID: "import-1", Artifact: id, Name: "first", Origin: "file:///a", Producer: "run-1", Metadata: map[string]json.RawMessage{"lab": json.RawMessage(`"x"`)}})
	if err != nil || again.Sequence != one.Sequence {
		t.Fatal(again, err)
	}
	_, err = f.service.RecordOccurrence(ctx, OccurrenceRequest{ID: "import-1", Artifact: id, Name: "changed"})
	var outcome *OccurrenceError
	if !errors.Is(err, ErrConflict) || !errors.As(err, &outcome) || outcome.Committed {
		t.Fatal(err)
	}
	if _, err := f.service.RecordOccurrence(ctx, OccurrenceRequest{Artifact: "sha256:" + ID(strings.Repeat("0", 64))}); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	// Internal occurrences stay out of default discovery.
	hidden, err := f.service.RecordOccurrence(observability.WithInternal(ctx), OccurrenceRequest{Artifact: id})
	if err != nil {
		t.Fatal(err)
	}
	if ids := all(OccurrenceQuery{Artifact: id}); len(ids) != 2 {
		t.Fatal(ids)
	}
	if ids := all(OccurrenceQuery{Artifact: id, IncludeInternal: true}); len(ids) != 3 || ids[2] != hidden.ID {
		t.Fatal(ids)
	}

	if err := f.service.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := New(f.content, Options{Root: f.root, IndexedMetadata: []string{"lab"}})
	if err != nil {
		t.Fatal(err)
	}
	f.service = reopened
	t.Cleanup(func() { reopened.Close() })
	page2, err := reopened.Occurrences(ctx, OccurrenceQuery{Content: one.Content})
	if err != nil || len(page2.Occurrences) != 3 {
		t.Fatal(page2, err)
	}
}

func TestSequencesLimitsAndChangesSurviveReopen(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	if err := f.service.Close(); err != nil {
		t.Fatal(err)
	}
	service, err := New(f.content, Options{Root: f.root, MaxRecords: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { service.Close() }()
	stream := make(chan uint64, 1)
	stop := service.OnCommit(func(sequence uint64) {
		select {
		case stream <- sequence:
		default:
		}
	})
	defer stop()
	id := publish(t, service, ctx, f.manifest(t, "a", "1"))
	if sequence, err := service.Sequence(ctx); err != nil || sequence != 1 {
		t.Fatal(sequence, err)
	}
	if got := <-stream; got != 1 {
		t.Fatal(got)
	}
	// A replay commits nothing and does not move the sequence.
	publish(t, service, ctx, f.manifest(t, "a", "1"))
	if sequence, _ := service.Sequence(ctx); sequence != 1 {
		t.Fatal(sequence)
	}
	if _, err := service.RecordOccurrence(ctx, OccurrenceRequest{ID: "o", Artifact: id, Name: "n", Producer: "p"}); err != nil {
		t.Fatal(err)
	}
	publish(t, service, ctx, f.manifest(t, "b", "2"))
	if _, err := service.Publish(ctx, f.manifest(t, "c", "3")); !errors.Is(err, ErrLimit) {
		t.Fatal(err)
	}
	item, err := service.Search(ctx, CatalogQuery{ID: id})
	if err != nil || item.Items[0].Sequence != 1 {
		t.Fatal(item, err)
	}
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
	service, err = New(f.content, Options{Root: f.root, MaxRecords: 4})
	if err != nil {
		t.Fatal(err)
	}
	f.service = service
	if sequence, _ := service.Sequence(ctx); sequence != 3 {
		t.Fatal(sequence)
	}
	publish(t, service, ctx, f.manifest(t, "c", "3"))
	page, err := service.Changes(ctx, provenance.ChangeQuery{After: 1})
	if err != nil {
		t.Fatal(err)
	}
	classes := []string{}
	for _, change := range page.Changes {
		classes = append(classes, fmt.Sprintf("%d:%s", change.Sequence, change.Class))
	}
	if !reflect.DeepEqual(classes, []string{"2:occurrence", "3:artifact", "4:artifact"}) || page.Last != 4 {
		t.Fatal(classes, page.Last)
	}
	if page.Changes[0].ID != "o" || page.Changes[0].Kind != "n" || page.Changes[0].Activity != "p" {
		t.Fatal(page.Changes[0])
	}
}

func TestLeaseRefusalIsMachineCheckable(t *testing.T) {
	f := newFixture(t)
	_, err := New(f.content, Options{Root: f.root})
	var held *LeaseError
	if !errors.Is(err, ErrLeaseHeld) || !errors.As(err, &held) || held.Owner.PID != os.Getpid() {
		t.Fatal(err)
	}
}

func TestSeekableAccessVerifiesBeforeServing(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	content := strings.Repeat("0123456789", 10000)
	id := publish(t, f.service, ctx, f.manifest(t, "big.bin", content))
	file, err := f.service.OpenSeekable(ctx, id, "big.bin")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Seek(12345, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 10)
	if _, err := io.ReadFull(file, buffer); err != nil || string(buffer) != content[12345:12355] {
		t.Fatal(string(buffer), err)
	}
	if _, err := file.ReadAt(buffer, 777); err != nil || string(buffer) != content[777:787] {
		t.Fatal(string(buffer), err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := f.service.Stat(ctx, id, "big.bin")
	if err != nil || info.Size != int64(len(content)) {
		t.Fatal(info, err)
	}

	// Scenario 10: one corrupt byte fires the documented path in both read styles.
	path := blobPath(f.blobRoot, info.Content)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)/2] ^= 0xff
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.OpenSeekable(ctx, id, "big.bin"); !errors.Is(err, cas.ErrIntegrity) {
		t.Fatalf("seekable open of corrupt content: %v", err)
	}
	stream, err := f.service.OpenFile(ctx, id, "big.bin")
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if _, err := io.Copy(io.Discard, stream); !errors.Is(err, cas.ErrIntegrity) {
		t.Fatalf("stream of corrupt content: %v", err)
	}

	// A backend with no capability says so distinctly.
	f.service.blobs = &countingCAS{base: f.content}
	if _, err := f.service.OpenSeekable(ctx, id, "big.bin"); !errors.Is(err, cas.ErrCapability) {
		t.Fatal(err)
	}
}

func blobPath(root string, id cas.ID) string {
	digest := strings.TrimPrefix(string(id), "sha256:")
	return filepath.Join(root, digest[:2], digest)
}

func TestReachableCoversRootsBlocksAndFiles(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	manifest := f.manifest(t, "a", "content")
	id := publish(t, f.service, ctx, manifest)
	reached := map[cas.ID]bool{}
	if err := f.service.Reachable(ctx, func(c cas.ID) error { reached[c] = true; return nil }); err != nil {
		t.Fatal(err)
	}
	root, err := f.service.loadRoot(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []cas.ID{cas.ID(id), root.Blocks[0].ID, manifest.Entries[0].Content} {
		if !reached[want] {
			t.Fatalf("missing %s in %v", want, reached)
		}
	}
	// Fence runs its function exclusive of publication.
	ran := false
	if err := f.service.Fence(ctx, func(context.Context) error { ran = true; return nil }); err != nil || !ran {
		t.Fatal(ran, err)
	}
}
