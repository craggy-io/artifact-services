package artifacts

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/craggy-io/artifact-services/internal/pager"
	"github.com/craggy-io/artifact-services/observability"
)

// mapPageError joins public sentinels for internal paging failures so external
// consumers can match them with errors.Is.
func mapPageError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, pager.ErrCursor):
		return errors.Join(ErrCursor, ErrInvalidQuery, err)
	case errors.Is(err, pager.ErrCapacity):
		return errors.Join(ErrLimit, err)
	default:
		return err
	}
}

func normalizeLimit(limit int) (int, error) {
	if limit < 0 || limit > 1000 {
		return 0, fmt.Errorf("%w: limit", ErrInvalidQuery)
	}
	if limit == 0 {
		return 100, nil
	}
	return limit, nil
}

func (s *Service) ListFiles(ctx context.Context, id ID, query FileQuery) (page FilePage, resultErr error) {
	ctx, span := start(ctx, "list_files", string(id))
	defer func() { span.End(resultErr) }()
	if err := ctx.Err(); err != nil {
		return page, err
	}
	if err := s.requireOpen(); err != nil {
		return page, err
	}
	if _, err := ParseID(string(id)); err != nil {
		return page, err
	}
	limit, err := normalizeLimit(query.Limit)
	if err != nil {
		return page, err
	}
	query.Limit = limit
	if query.Prefix != "" {
		directory := strings.TrimSuffix(query.Prefix, "/")
		if !validPath(directory) {
			return page, fmt.Errorf("%w: directory prefix", ErrInvalidQuery)
		}
		query.Prefix = directory + "/"
	}
	cursor := query.Cursor
	query.Cursor = ""
	entries, next, err := s.files.Page(struct {
		ID    ID
		Query FileQuery
	}{id, query}, cursor, limit, func() ([]Entry, error) {
		manifest, err := s.Get(observability.WithInternal(ctx), id)
		if err != nil {
			return nil, err
		}
		entries := []Entry{}
		for _, entry := range manifest.Entries {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if strings.HasPrefix(entry.Path, query.Prefix) {
				entries = append(entries, entry)
			}
		}
		return entries, nil
	})
	if err != nil {
		return page, mapPageError(err)
	}
	if err := ctx.Err(); err != nil {
		return page, err
	}
	span.Count("returned", int64(len(entries)))
	return FilePage{Entries: entries, NextCursor: next}, nil
}

func (s *Service) normalizeQuery(query CatalogQuery) (CatalogQuery, error) {
	limit, err := normalizeLimit(query.Limit)
	if err != nil {
		return query, err
	}
	query.Limit = limit
	for _, id := range []ID{query.ID, query.SchemaID} {
		if id != "" {
			if _, err := ParseID(string(id)); err != nil {
				return query, fmt.Errorf("%w: %w", ErrInvalidQuery, err)
			}
		}
	}
	for _, pair := range [][2]*int64{{query.MinFiles, query.MaxFiles}, {query.MinBytes, query.MaxBytes}} {
		if pair[0] != nil && *pair[0] < 0 || pair[1] != nil && *pair[1] < 0 || pair[0] != nil && pair[1] != nil && *pair[0] > *pair[1] {
			return query, fmt.Errorf("%w: bounds", ErrInvalidQuery)
		}
	}
	if len(query.Metadata) > 16 {
		return query, fmt.Errorf("%w: predicate count", ErrInvalidQuery)
	}
	predicates := map[string]json.RawMessage{}
	for key, value := range query.Metadata {
		if !s.indexed[key] {
			return query, fmt.Errorf("%w: unconfigured key %q", ErrInvalidQuery, key)
		}
		normalized, err := indexedScalar(value)
		if err != nil {
			return query, fmt.Errorf("%w: metadata %q: %w", ErrInvalidQuery, key, err)
		}
		predicates[key] = normalized
	}
	query.Metadata = predicates
	return query, nil
}
func within(value int64, min, max *int64) bool {
	return (min == nil || value >= *min) && (max == nil || value <= *max)
}
func matches(member membership, query CatalogQuery) bool {
	if member.Category == observability.Internal && !query.IncludeInternal {
		return false
	}
	if query.ID != "" && query.ID != member.ID || query.SchemaID != "" && query.SchemaID != member.SchemaID {
		return false
	}
	if !within(member.FileCount, query.MinFiles, query.MaxFiles) || !within(member.LogicalBytes, query.MinBytes, query.MaxBytes) {
		return false
	}
	for key, value := range query.Metadata {
		if !bytes.Equal(member.Metadata[key], value) {
			return false
		}
	}
	return true
}

func (s *Service) Search(ctx context.Context, query CatalogQuery) (page CatalogPage, resultErr error) {
	ctx, span := start(ctx, "search", "")
	defer func() { span.End(resultErr) }()
	if err := ctx.Err(); err != nil {
		return page, err
	}
	if err := s.requireOpen(); err != nil {
		return page, err
	}
	query, err := s.normalizeQuery(query)
	if err != nil {
		return page, err
	}
	cursor := query.Cursor
	query.Cursor = ""
	items, next, err := s.searches.Page(query, cursor, query.Limit, func() ([]CatalogItem, error) {
		// Snapshot under the same lock used for commit+projection. Later appends cannot
		// enter this cursor's stable result set; no asynchronous index lag is exposed.
		s.mu.RLock()
		members := make([]membership, 0, len(s.catalog))
		for _, m := range s.catalog {
			members = append(members, m)
		}
		s.mu.RUnlock()
		items := []CatalogItem{}
		for _, member := range members {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if matches(member, query) {
				items = append(items, member.CatalogItem)
			}
		}
		sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
		return items, nil
	})
	if err != nil {
		return page, mapPageError(err)
	}
	if err := ctx.Err(); err != nil {
		return page, err
	}
	span.Count("returned", int64(len(items)))
	return CatalogPage{Items: items, NextCursor: next}, nil
}

func (s *Service) OpenFile(ctx context.Context, id ID, path string) (reader io.ReadCloser, resultErr error) {
	ctx, span := start(ctx, "open_file", string(id))
	defer func() {
		if resultErr != nil {
			span.End(resultErr)
		}
	}()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !validPath(path) {
		return nil, fmt.Errorf("%w: file path", ErrInvalidQuery)
	}
	manifest, err := s.Get(observability.WithInternal(ctx), id)
	if err != nil {
		return nil, err
	}
	index := sort.Search(len(manifest.Entries), func(i int) bool { return manifest.Entries[i].Path >= path })
	if index == len(manifest.Entries) || manifest.Entries[index].Path != path {
		return nil, ErrNotFound
	}
	source, err := s.blobs.Open(ctx, manifest.Entries[index].Content)
	if err != nil {
		return nil, err
	}
	return &fileReader{source: source, span: span}, nil
}

type fileReader struct {
	source      io.ReadCloser
	span        *observability.Span
	closed, eof bool
	closeErr    error
	terminal    error
}

func (r *fileReader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, io.ErrClosedPipe
	}
	if r.terminal != nil {
		return 0, r.terminal
	}
	n, err := r.source.Read(p)
	r.span.AddBytes(int64(n))
	if err == io.EOF {
		if !r.eof {
			r.eof = true
			r.span.Count("eof", 1)
		}
	} else if err != nil {
		r.terminal = err
		r.span.RecordError(err)
	}
	return n, err
}
func (r *fileReader) Close() error {
	if r.closed {
		return r.closeErr
	}
	r.closed = true
	r.closeErr = r.source.Close()
	r.span.Count("closed", 1)
	if !r.eof {
		r.span.Count("early_closed", 1)
	}
	r.span.End(r.closeErr)
	return r.closeErr
}
