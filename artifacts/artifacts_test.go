package artifacts

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/craggy-io/artifact-services/cas"
	"github.com/craggy-io/artifact-services/internal/canon"
	"github.com/craggy-io/artifact-services/internal/typedclaim"
	"github.com/craggy-io/artifact-services/observability"
	"github.com/craggy-io/artifact-services/storage"
)

type fixture struct {
	service        *Service
	content        *cas.Service
	root, blobRoot string
}

func newFixture(t *testing.T, keys ...string) fixture {
	t.Helper()
	base := t.TempDir()
	blobRoot := filepath.Join(base, "blobs")
	blob, err := storage.NewFilesystem(blobRoot)
	if err != nil {
		t.Fatal(err)
	}
	content, err := cas.New(blob, cas.Options{TempDir: base})
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(base, "catalog")
	service, err := New(content, Options{Root: root, IndexedMetadata: keys})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := service.Close(); err != nil {
			t.Error(err)
		}
	})
	return fixture{service: service, content: content, root: root, blobRoot: blobRoot}
}
func (f fixture) manifest(t *testing.T, path, value string) Manifest {
	t.Helper()
	put, err := f.content.Put(context.Background(), strings.NewReader(value))
	if err != nil {
		t.Fatal(err)
	}
	return Manifest{Entries: []Entry{{Path: path, Content: put.ID, Size: put.Size}}}
}
func publish(t *testing.T, s *Service, ctx context.Context, m Manifest) ID {
	t.Helper()
	id, err := s.Publish(ctx, m)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func readFile(t *testing.T, s *Service, id ID, path string) string {
	t.Helper()
	r, err := s.OpenFile(context.Background(), id, path)
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(r)
	closeErr := r.Close()
	if err != nil || closeErr != nil {
		t.Fatal(err, closeErr)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	return string(b)
}
func search(t *testing.T, s *Service, q CatalogQuery) CatalogPage {
	t.Helper()
	p, err := s.Search(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestCanonicalPublishRoundTripAndReopen(t *testing.T) {
	f := newFixture(t, "kind")
	m := f.manifest(t, "dir/b", "content")
	m.Entries = append(m.Entries, Entry{Path: "dir/a", Content: m.Entries[0].Content, Size: 7})
	m.Metadata = map[string]json.RawMessage{"kind": json.RawMessage(`"apk"`), "value": json.RawMessage(`{"z":1.0,"a":true}`)}
	id := publish(t, f.service, context.Background(), m)
	m.Entries[0], m.Entries[1] = m.Entries[1], m.Entries[0]
	m.Metadata["value"] = json.RawMessage(`{"a":true,"z":1}`)
	if duplicate := publish(t, f.service, context.Background(), m); duplicate != id {
		t.Fatal("canonical identity changed")
	}
	got, err := f.service.Get(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != 1 || got.Entries[0].Path != "dir/a" {
		t.Fatal(got)
	}
	if readFile(t, f.service, id, "dir/b") != "content" {
		t.Fatal("payload")
	}
	page := search(t, f.service, CatalogQuery{Metadata: map[string]json.RawMessage{"kind": json.RawMessage(`"apk"`)}})
	if len(page.Items) != 1 || page.Items[0].FileCount != 2 || page.Items[0].LogicalBytes != 14 {
		t.Fatal(page)
	}
	if err := f.service.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := New(f.content, Options{Root: f.root, IndexedMetadata: []string{"kind"}})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if readFile(t, reopened, id, "dir/a") != "content" || len(search(t, reopened, CatalogQuery{}).Items) != 1 {
		t.Fatal("reopen")
	}
}

func TestValidationAndIntegrity(t *testing.T) {
	for _, path := range []string{"", "../x", "/abs", "a\\b", "a//b", "a/./b", "a/../b", "a:b", "a/"} {
		t.Run(path, func(t *testing.T) {
			f := newFixture(t)
			m := f.manifest(t, path, "x")
			if _, err := f.service.Publish(context.Background(), m); !errors.Is(err, ErrInvalidManifest) {
				t.Fatal(err)
			}
		})
	}
	f := newFixture(t, "kind")
	base := f.manifest(t, "file", "abc")
	for _, modify := range []func(*Manifest){func(m *Manifest) { m.Version = 2 }, func(m *Manifest) { m.Entries = append(m.Entries, m.Entries[0]) }, func(m *Manifest) { m.Entries[0].Size = 2 }, func(m *Manifest) { m.Entries[0].Content = "sha256:bad" }, func(m *Manifest) { m.Metadata = map[string]json.RawMessage{"kind": json.RawMessage(`[]`)} }, func(m *Manifest) { m.Metadata = map[string]json.RawMessage{"kind": json.RawMessage(`1.5`)} }, func(m *Manifest) { m.Metadata = map[string]json.RawMessage{"value": json.RawMessage(`{"x":1,"x":2}`)} }} {
		m := Manifest{Entries: append([]Entry(nil), base.Entries...)}
		modify(&m)
		if _, err := f.service.Publish(context.Background(), m); err == nil {
			t.Fatal("invalid accepted")
		}
	}
	// Corrupt a referenced blob on disk: complete manifest publication must fail.
	path := filepath.Join(f.blobRoot, string(base.Entries[0].Content)[7:9], string(base.Entries[0].Content)[7:])
	if err := os.WriteFile(path, []byte("abd"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.Publish(context.Background(), base); !errors.Is(err, cas.ErrIntegrity) {
		t.Fatal(err)
	}
	if len(search(t, f.service, CatalogQuery{}).Items) != 0 {
		t.Fatal("incomplete manifest registered")
	}
}

func TestTypedClaimCannotBeSpoofed(t *testing.T) {
	f := newFixture(t)
	source := f.manifest(t, "rows.jsonl", "{\"id\":1}\n")
	dataID := publish(t, f.service, context.Background(), source)
	schemaID := publish(t, f.service, context.Background(), f.manifest(t, "schema.json", `{"version":1}`))
	bad := source
	bad.Metadata = map[string]json.RawMessage{"objects.schema": json.RawMessage(fmt.Sprintf("%q", schemaID))}
	if _, err := f.service.Publish(context.Background(), bad); !errors.Is(err, ErrInvalidManifest) {
		t.Fatal(err)
	}
	if _, err := f.service.PublishValidated(context.Background(), source, typedclaim.Claim{}); !errors.Is(err, ErrUnvalidated) {
		t.Fatal(err)
	}
	forged := storeUnregistered(t, f, bad)
	if _, err := f.service.ValidatedClaim(context.Background(), forged); !errors.Is(err, ErrUnvalidated) {
		t.Fatal("CAS metadata forged accepted claim", err)
	}
	claim := typedclaim.New(string(schemaID), string(dataID), "objects-v1", "jsonl-v1")
	typedID, err := f.service.PublishValidated(context.Background(), source, claim)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := f.service.ValidatedClaim(context.Background(), typedID)
	if err != nil {
		t.Fatal(err)
	}
	schema, data, validator, encoding, valid := accepted.Fields()
	if !valid || schema != string(schemaID) || data != string(dataID) || validator != "objects-v1" || encoding != "jsonl-v1" {
		t.Fatal("claim mismatch")
	}
	if page := search(t, f.service, CatalogQuery{SchemaID: schemaID}); len(page.Items) != 1 || page.Items[0].ID != typedID {
		t.Fatal(page)
	}
	wrong := f.manifest(t, "different", "other")
	if _, err := f.service.PublishValidated(context.Background(), wrong, claim); !errors.Is(err, ErrUnvalidated) {
		t.Fatal("claim changed data", err)
	}
	if err := f.service.Rebuild(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSnapshotPaginationAndClassification(t *testing.T) {
	f := newFixture(t, "n")
	for i := 0; i < 4; i++ {
		m := f.manifest(t, "file", fmt.Sprint(i))
		m.Metadata = map[string]json.RawMessage{"n": json.RawMessage(fmt.Sprint(i))}
		publish(t, f.service, context.Background(), m)
	}
	hidden := f.manifest(t, "file", "internal")
	publish(t, f.service, observability.WithInternal(context.Background()), hidden)
	first := search(t, f.service, CatalogQuery{Limit: 2})
	if first.NextCursor == "" {
		t.Fatal("no cursor")
	}
	publish(t, f.service, context.Background(), f.manifest(t, "file", "later"))
	second := search(t, f.service, CatalogQuery{Limit: 2, Cursor: first.NextCursor})
	if len(second.Items) != 2 || second.NextCursor != "" {
		t.Fatal(second)
	}
	replay := search(t, f.service, CatalogQuery{Limit: 2, Cursor: first.NextCursor})
	a, _ := canon.Marshal(second)
	b, _ := canon.Marshal(replay)
	if !bytes.Equal(a, b) {
		t.Fatal("cursor replay changed")
	}
	if len(search(t, f.service, CatalogQuery{}).Items) != 5 || len(search(t, f.service, CatalogQuery{IncludeInternal: true}).Items) != 6 {
		t.Fatal("classification")
	}
	if _, err := f.service.Search(context.Background(), CatalogQuery{Limit: 2, Cursor: first.NextCursor, IncludeInternal: true}); err == nil {
		t.Fatal("cursor filters changed")
	}
	if _, err := f.service.Publish(context.Background(), hidden); !errors.Is(err, ErrConflict) {
		t.Fatal("classification changed", err)
	}
	for _, q := range []CatalogQuery{{Limit: -1}, {Limit: 1001}, {Metadata: map[string]json.RawMessage{"unknown": json.RawMessage(`1`)}}, {Metadata: map[string]json.RawMessage{"n": json.RawMessage(`{}`)}}} {
		if _, err := f.service.Search(context.Background(), q); !errors.Is(err, ErrInvalidQuery) {
			t.Fatal(err)
		}
	}
}

func TestFileListingAndReaderLifecycle(t *testing.T) {
	f := newFixture(t)
	m := f.manifest(t, "dir/a", "abc")
	m.Entries = append(m.Entries, Entry{Path: "directory/b", Content: m.Entries[0].Content, Size: 3}, Entry{Path: "dir/c", Content: m.Entries[0].Content, Size: 3})
	id := publish(t, f.service, context.Background(), m)
	first, err := f.service.ListFiles(context.Background(), id, FileQuery{Prefix: "dir", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Entries) != 1 || first.Entries[0].Path != "dir/a" || first.NextCursor == "" {
		t.Fatal(first)
	}
	second, err := f.service.ListFiles(context.Background(), id, FileQuery{Prefix: "dir/", Limit: 1, Cursor: first.NextCursor})
	if err != nil || len(second.Entries) != 1 || second.Entries[0].Path != "dir/c" {
		t.Fatal(second, err)
	}
	if _, err := f.service.OpenFile(context.Background(), id, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	var metrics observability.Collector
	ctx := observability.With(context.Background(), observability.Options{Collector: &metrics})
	r, err := f.service.OpenFile(ctx, id, "dir/a")
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Read(make([]byte, 1)); err != io.ErrClosedPipe {
		t.Fatal(err)
	}
	result := metrics.Snapshot(observability.SnapshotOptions{})[observability.Scope{Layer: "artifacts", Operation: "open_file"}]
	if result.Completed != 1 || result.Counters["early_closed"] != 1 {
		t.Fatal(result)
	}
}

type afterPut struct {
	cas.Store
	after func()
	err   error
}

func (s afterPut) Put(ctx context.Context, r io.Reader) (cas.PutResult, error) {
	result, err := s.Store.Put(ctx, r)
	if err != nil {
		return result, err
	}
	if s.after != nil {
		s.after()
	}
	if s.err != nil {
		return result, &cas.PutError{Committed: true, Err: s.err}
	}
	return result, nil
}
func TestCommittedFailureStages(t *testing.T) {
	t.Run("CAS cleanup", func(t *testing.T) {
		f := newFixture(t)
		m := f.manifest(t, "file", "abc")
		failure := errors.New("cleanup")
		f.service.blobs = afterPut{Store: f.content, err: failure}
		id, err := f.service.Publish(context.Background(), m)
		var p *PublishError
		if !errors.As(err, &p) || !p.ManifestCommitted || !p.Registered || p.ID != id || !errors.Is(err, failure) {
			t.Fatal(id, err)
		}
		if len(search(t, f.service, CatalogQuery{}).Items) != 1 {
			t.Fatal("committed membership lost")
		}
	})
	t.Run("registration", func(t *testing.T) {
		f := newFixture(t)
		m := f.manifest(t, "file", "abc")
		f.service.blobs = afterPut{Store: f.content, after: func() {
			if err := f.service.Close(); err != nil {
				t.Fatal(err)
			}
		}}
		id, err := f.service.Publish(context.Background(), m)
		var p *PublishError
		if !errors.As(err, &p) || !p.ManifestCommitted || p.Registered {
			t.Fatal(err)
		}
		reopened, err := New(f.content, Options{Root: f.root})
		if err != nil {
			t.Fatal(err)
		}
		defer reopened.Close()
		if _, err := reopened.Get(context.Background(), id); err != nil {
			t.Fatal(err)
		}
		if len(search(t, reopened, CatalogQuery{}).Items) != 0 {
			t.Fatal("failed registration visible")
		}
		if retry := publish(t, reopened, context.Background(), m); retry != id {
			t.Fatal("retry identity")
		}
	})
}

func TestConcurrentPublishAndCancellation(t *testing.T) {
	f := newFixture(t)
	m := f.manifest(t, "file", "content")
	var wg sync.WaitGroup
	for range 12 {
		wg.Go(func() {
			if _, err := f.service.Publish(context.Background(), m); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if len(search(t, f.service, CatalogQuery{}).Items) != 1 {
		t.Fatal("duplicate memberships")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := f.service.Publish(ctx, m); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := f.service.Get(ctx, "bad"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := f.service.Search(ctx, CatalogQuery{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestGetRejectsNoncanonicalAndPolicyChanges(t *testing.T) {
	f := newFixture(t, "kind")
	m := f.manifest(t, "file", "x")
	raw, err := json.MarshalIndent(m, "", " ")
	if err != nil {
		t.Fatal(err)
	}
	put, err := f.content.Put(context.Background(), bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.service.Get(context.Background(), ID(put.ID)); !errors.Is(err, ErrInvalidManifest) {
		t.Fatal(err)
	}
	if _, err := New(f.content, Options{Root: f.root, IndexedMetadata: []string{"kind"}}); err == nil {
		t.Fatal("second live owner allowed")
	}
	if err := f.service.Close(); err != nil {
		t.Fatal(err)
	}
	if other, err := New(f.content, Options{Root: f.root, IndexedMetadata: []string{"other"}}); err == nil {
		other.Close()
		t.Fatal("policy changed without rebuild")
	}
}

func TestManifestBounds(t *testing.T) {
	f := newFixture(t)
	base := f.manifest(t, "file", "x")
	oversized := base
	f.service.options.MaxEntries = 1
	oversized.Entries = make([]Entry, 2)
	if _, err := f.service.Publish(context.Background(), oversized); !errors.Is(err, ErrInvalidManifest) {
		t.Fatal(err)
	}
	f.service.options.MaxEntries = defaultMaxEntries
	oversized = base
	oversized.Metadata = map[string]json.RawMessage{"large": json.RawMessage(`"` + strings.Repeat("a", maxMetadataBytes) + `"`)}
	if _, err := f.service.Publish(context.Background(), oversized); !errors.Is(err, ErrInvalidManifest) {
		t.Fatal(err)
	}
	if page := search(t, f.service, CatalogQuery{}); len(page.Items) != 0 {
		t.Fatal("bounded failures registered")
	}
}

func TestAuthoritativeCategory(t *testing.T) {
	f := newFixture(t)
	m := f.manifest(t, "file", "data")
	id := publish(t, f.service, observability.WithInternal(context.Background()), m)
	category, err := f.service.Category(context.Background(), id)
	if err != nil || category != observability.Internal {
		t.Fatal(category, err)
	}
	// Category is backed by immutable registration even if the derived view needs rebuild.
	f.service.mu.Lock()
	f.service.index = newCatalogIndex()
	f.service.mu.Unlock()
	if category, err := f.service.Category(context.Background(), id); err != nil || category != observability.Internal {
		t.Fatal(category, err)
	}
	if err := f.service.Rebuild(context.Background()); err != nil {
		t.Fatal(err)
	}
	raw := f.manifest(t, "other", "unregistered")
	blob := storeUnregistered(t, f, raw)
	if _, err := f.service.Category(context.Background(), blob); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if _, err := f.service.Get(context.Background(), blob); err != nil {
		t.Fatal(err)
	}
}

// storeUnregistered puts a manifest directly into CAS, as code holding only the
// CAS handle could, bypassing every catalog check.
func storeUnregistered(t *testing.T, f fixture, m Manifest) ID {
	t.Helper()
	ctx := context.Background()
	writer := &blockWriter{}
	writer.flush = func(encoded []byte, first string, count int) error {
		put, err := f.content.Put(ctx, bytes.NewReader(encoded))
		writer.root.Blocks = append(writer.root.Blocks, blockRef{ID: put.ID, First: first, Count: count})
		return err
	}
	for _, e := range m.Entries {
		if err := writer.add(e); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.finish(); err != nil {
		t.Fatal(err)
	}
	writer.root.Metadata = m.Metadata
	encoded, err := canon.Marshal(writer.root)
	if err != nil {
		t.Fatal(err)
	}
	blob, err := f.content.Put(ctx, bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	return ID(blob.ID)
}
