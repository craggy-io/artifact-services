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

	"github.com/craggy-io/artifact-services/cas"
	"github.com/craggy-io/artifact-services/internal/canon"
	"github.com/craggy-io/artifact-services/internal/journal"
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

func startBlock(root storedManifest, after string) int {
	if after == "" {
		return 0
	}
	// The last block whose first path is at or before the position may contain
	// entries after it, or a link whose subtree the position lies inside.
	index := sort.Search(len(root.Blocks), func(i int) bool { return comparePaths(root.Blocks[i].First, after) > 0 }) - 1
	return max(index, 0)
}

// walk visits entries strictly after the position, in path order, until visit
// reports stop. seek positions the scan without excluding entries past it: it is
// used for a prefix, whose directory entry itself the caller filters. With expand,
// a link yields the entries it embeds, renamed under the link's path. A position
// can lie inside a link, in which case the walk resumes inside that link first.
func (s *Service) walk(ctx context.Context, root storedManifest, seek, after string, expand bool, depth int, visit func(Entry) (bool, error)) error {
	if depth > s.options.MaxLinkDepth {
		return invalidf("link depth")
	}
	position := after
	if comparePaths(seek, position) > 0 {
		position = seek
	}
	if expand && position != "" {
		stop, err := s.resumeInsideLink(ctx, root, position, depth, visit)
		if stop || err != nil {
			return err
		}
	}
	for index := startBlock(root, position); index < len(root.Blocks); index++ {
		entries, err := s.loadBlock(ctx, root.Blocks[index])
		if err != nil {
			return err
		}
		for at := sort.Search(len(entries), func(i int) bool { return comparePaths(entries[i].Path, position) >= 0 }); at < len(entries); at++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			e := entries[at]
			if after != "" && comparePaths(e.Path, after) <= 0 {
				continue
			}
			var stop bool
			if expand && e.Kind == KindLink {
				stop, err = s.walkLink(ctx, e, "", depth, visit)
			} else {
				stop, err = visit(e)
			}
			if stop || err != nil {
				return err
			}
		}
	}
	return nil
}

// resumeInsideLink finishes any link the position lies inside. The ordinary scan
// starts at the position, past that link's own entry, so it would never revisit
// it. Nothing can nest under a link, so at most one ancestor qualifies.
func (s *Service) resumeInsideLink(ctx context.Context, root storedManifest, position string, depth int, visit func(Entry) (bool, error)) (bool, error) {
	for ancestor := position; ; {
		slash := strings.LastIndexByte(ancestor, '/')
		if slash < 0 {
			return false, nil
		}
		ancestor = ancestor[:slash]
		e, found, err := s.findEntry(ctx, root, ancestor)
		if err != nil {
			return false, err
		}
		if found {
			if e.Kind == KindLink {
				return s.walkLink(ctx, e, position, depth, visit)
			}
			return false, nil
		}
	}
}

func (s *Service) walkLink(ctx context.Context, link Entry, position string, depth int, visit func(Entry) (bool, error)) (bool, error) {
	target, err := s.loadRoot(ctx, link.Target)
	if err != nil {
		return false, err
	}
	// Map the position into the target's namespace.
	base := link.TargetPath
	inner := ""
	if rest, ok := strings.CutPrefix(position, link.Path+"/"); ok {
		inner = rest
		if base != "" {
			inner = base + "/" + rest
		}
	}
	if base != "" && inner == "" {
		// A sub-path link to one file is that file, under the link's name.
		if info, err := s.resolvePath(ctx, link.Target, base, depth+1); err == nil && info.Kind == "" {
			return visit(Entry{Path: link.Path, Content: info.Content, Size: info.Size, Mode: info.Mode})
		}
		inner = base
	}
	stopped := false
	err = s.walk(ctx, target, inner, inner, true, depth+1, func(e Entry) (bool, error) {
		rest := e.Path
		if base != "" {
			var ok bool
			if rest, ok = strings.CutPrefix(e.Path, base+"/"); !ok {
				stopped = false
				return true, nil
			}
		}
		e.Path = link.Path + "/" + rest
		stop, err := visit(e)
		stopped = stop
		return stop, err
	})
	return stopped, err
}

func (s *Service) ListFiles(ctx context.Context, id ID, query FileQuery) (page FilePage, resultErr error) {
	ctx, span := start(ctx, "list_files", string(id))
	defer func() { span.End(resultErr) }()
	if err := ctx.Err(); err != nil {
		return page, err
	}
	if err := s.requireOpen(ctx); err != nil {
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
	directory := ""
	if query.Prefix != "" {
		directory = strings.TrimSuffix(query.Prefix, "/")
		if !validPath(directory) {
			return page, fmt.Errorf("%w: directory prefix", ErrInvalidQuery)
		}
		query.Prefix = directory + "/"
	}
	cursor := query.Cursor
	query.Cursor = ""
	binding := struct {
		ID    ID
		Query FileQuery
	}{id, query}
	after := ""
	if cursor != "" {
		if after, err = s.files.OpenPosition(binding, cursor); err != nil {
			return page, mapPageError(err)
		}
	}
	root, err := s.loadRoot(observability.WithInternal(ctx), id)
	if err != nil {
		return page, err
	}
	entries := make([]Entry, 0, limit)
	more := false
	err = s.walk(observability.WithInternal(ctx), root, directory, after, query.Expand, 0, func(e Entry) (bool, error) {
		if query.Prefix != "" && !strings.HasPrefix(e.Path, query.Prefix) {
			if comparePaths(e.Path, query.Prefix) > 0 {
				return true, nil
			}
			return false, nil
		}
		if len(entries) == limit {
			more = true
			return true, nil
		}
		entries = append(entries, e)
		return false, nil
	})
	if err != nil {
		return page, err
	}
	if err := ctx.Err(); err != nil {
		return page, err
	}
	page.Entries = entries
	if more {
		if page.NextCursor, err = s.files.Position(binding, entries[len(entries)-1].Path); err != nil {
			return FilePage{}, err
		}
	}
	span.Count("returned", int64(len(entries)))
	return page, nil
}

// findEntry reads the root and at most one block.
func (s *Service) findEntry(ctx context.Context, root storedManifest, path string) (Entry, bool, error) {
	index := sort.Search(len(root.Blocks), func(i int) bool { return comparePaths(root.Blocks[i].First, path) > 0 }) - 1
	if index < 0 {
		return Entry{}, false, nil
	}
	entries, err := s.loadBlock(ctx, root.Blocks[index])
	if err != nil {
		return Entry{}, false, err
	}
	at := sort.Search(len(entries), func(i int) bool { return comparePaths(entries[i].Path, path) >= 0 })
	if at == len(entries) || entries[at].Path != path {
		return Entry{}, false, nil
	}
	return entries[at], true, nil
}

// resolvePath finds an entry, following any link along the path. It descends at
// most MaxLinkDepth links and reads one block per ancestor it must test.
func (s *Service) resolvePath(ctx context.Context, id ID, path string, depth int) (FileInfo, error) {
	if depth > s.options.MaxLinkDepth {
		return FileInfo{}, invalidf("link depth")
	}
	root, err := s.loadRoot(ctx, id)
	if err != nil {
		return FileInfo{}, err
	}
	for ancestor := path; ; {
		e, found, err := s.findEntry(ctx, root, ancestor)
		if err != nil {
			return FileInfo{}, err
		}
		if found {
			if ancestor == path && e.Kind != KindLink {
				return FileInfo{Path: path, Kind: e.Kind, Content: e.Content, Size: e.Size, Mode: e.Mode}, nil
			}
			if e.Kind == KindLink {
				inner := strings.TrimPrefix(strings.TrimPrefix(path, ancestor), "/")
				if e.TargetPath != "" {
					inner = strings.TrimSuffix(e.TargetPath+"/"+inner, "/")
				}
				if inner == "" {
					return FileInfo{Path: path, Kind: KindLink}, nil
				}
				info, err := s.resolvePath(ctx, e.Target, inner, depth+1)
				info.Path = path
				return info, err
			}
			// A file or directory ancestor holds no link, and this path is absent.
			return FileInfo{}, ErrNotFound
		}
		slash := strings.LastIndexByte(ancestor, '/')
		if slash < 0 {
			return FileInfo{}, ErrNotFound
		}
		ancestor = ancestor[:slash]
	}
}

// Stat resolves a path, following links, without reading file content. It reads
// the manifest root and the blocks holding the path.
func (s *Service) Stat(ctx context.Context, id ID, path string) (info FileInfo, resultErr error) {
	ctx, span := start(ctx, "stat", string(id))
	defer func() { span.End(resultErr) }()
	if err := ctx.Err(); err != nil {
		return FileInfo{}, err
	}
	if err := s.requireOpen(ctx); err != nil {
		return FileInfo{}, err
	}
	if !validPath(path) {
		return FileInfo{}, fmt.Errorf("%w: file path", ErrInvalidQuery)
	}
	return s.resolvePath(observability.WithInternal(ctx), id, path, 0)
}

func (s *Service) normalizePredicates(metadata map[string]json.RawMessage) (map[string]json.RawMessage, error) {
	if len(metadata) > 16 {
		return nil, fmt.Errorf("%w: predicate count", ErrInvalidQuery)
	}
	predicates := map[string]json.RawMessage{}
	for key, value := range metadata {
		if !s.indexed[key] {
			return nil, fmt.Errorf("%w: unconfigured key %q", ErrInvalidQuery, key)
		}
		normalized, err := canon.IndexedScalar(value)
		if err != nil {
			return nil, fmt.Errorf("%w: metadata %q: %w", ErrInvalidQuery, key, err)
		}
		predicates[key] = normalized
	}
	return predicates, nil
}

func (s *Service) normalizeQuery(query CatalogQuery) (CatalogQuery, error) {
	limit, err := normalizeLimit(query.Limit)
	if err != nil {
		return query, err
	}
	query.Limit = limit
	for _, id := range []string{string(query.ID), string(query.SchemaID), string(query.Content)} {
		if id != "" {
			if _, err := ParseID(id); err != nil {
				return query, fmt.Errorf("%w: %w", ErrInvalidQuery, err)
			}
		}
	}
	for _, pair := range [][2]*int64{{query.MinFiles, query.MaxFiles}, {query.MinBytes, query.MaxBytes}} {
		if pair[0] != nil && *pair[0] < 0 || pair[1] != nil && *pair[1] < 0 || pair[0] != nil && pair[1] != nil && *pair[0] > *pair[1] {
			return query, fmt.Errorf("%w: bounds", ErrInvalidQuery)
		}
	}
	predicates, err := s.normalizePredicates(query.Metadata)
	if err != nil {
		return query, err
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
	if query.ID != "" && query.ID != member.ID || query.SchemaID != "" && query.SchemaID != member.SchemaID || query.Content != "" && query.Content != member.Content {
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

// catalogMatches collects (ID, sequence) for a query. A predicate with an index is
// answered from it; only unindexed queries stream the whole journal.
func (s *Service) catalogMatches(ctx context.Context, query CatalogQuery, visit func(membership) error) error {
	if query.ID != "" {
		entry, ok, err := s.records.Get(string(query.ID))
		if err != nil || !ok {
			return err
		}
		member, err := decodeMembership(entry)
		if err != nil {
			return err
		}
		return visit(member)
	}
	s.mu.RLock()
	var lists [][]uint64
	if query.SchemaID != "" {
		lists = append(lists, s.index.schema.Snapshot(string(query.SchemaID)))
	}
	if query.Content != "" {
		lists = append(lists, s.index.content.Snapshot(string(query.Content)))
	}
	for key, value := range query.Metadata {
		lists = append(lists, s.index.metadata.Snapshot(journal.IndexKey(key, string(value))))
	}
	s.mu.RUnlock()
	if len(lists) > 0 {
		sort.Slice(lists, func(i, j int) bool { return len(lists[i]) < len(lists[j]) })
		for _, sequence := range lists[0] {
			if err := ctx.Err(); err != nil {
				return err
			}
			entry, err := s.records.At(sequence)
			if err != nil {
				return err
			}
			member, err := decodeMembership(entry)
			if err != nil {
				return err
			}
			if err := visit(member); err != nil {
				return err
			}
		}
		return nil
	}
	return s.records.Scan(ctx, 0, 0, func(entry journal.Entry) error {
		if strings.HasPrefix(entry.Key, occurrenceKeyPrefix) {
			return nil
		}
		member, err := decodeMembership(entry)
		if err != nil {
			return err
		}
		return visit(member)
	})
}

func (s *Service) Search(ctx context.Context, query CatalogQuery) (page CatalogPage, resultErr error) {
	ctx, span := start(ctx, "search", "")
	defer func() { span.End(resultErr) }()
	if err := ctx.Err(); err != nil {
		return page, err
	}
	if err := s.requireOpen(ctx); err != nil {
		return page, err
	}
	query, err := s.normalizeQuery(query)
	if err != nil {
		return page, err
	}
	cursor := query.Cursor
	query.Cursor = ""
	sequences, next, err := s.searches.Page(query, cursor, query.Limit, func() ([]uint64, error) {
		// Only sequences enter the snapshot: records are immutable, so later appends
		// cannot change a stable result set, and no asynchronous index lag is exposed.
		type hit struct {
			id       ID
			sequence uint64
		}
		hits := []hit{}
		err := s.catalogMatches(ctx, query, func(member membership) error {
			if matches(member, query) {
				hits = append(hits, hit{member.ID, member.Sequence})
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		sort.Slice(hits, func(i, j int) bool { return hits[i].id < hits[j].id })
		ordered := make([]uint64, len(hits))
		for i, h := range hits {
			ordered[i] = h.sequence
		}
		return ordered, nil
	})
	if err != nil {
		return page, mapPageError(err)
	}
	if err := ctx.Err(); err != nil {
		return page, err
	}
	items := make([]CatalogItem, 0, len(sequences))
	for _, sequence := range sequences {
		entry, err := s.records.At(sequence)
		if err != nil {
			return page, err
		}
		member, err := decodeMembership(entry)
		if err != nil {
			return page, err
		}
		items = append(items, member.CatalogItem)
	}
	span.Count("returned", int64(len(items)))
	return CatalogPage{Items: items, NextCursor: next}, nil
}

// resolveFile resolves a path to a regular file's content.
func (s *Service) resolveFile(ctx context.Context, id ID, path string) (FileInfo, error) {
	if !validPath(path) {
		return FileInfo{}, fmt.Errorf("%w: file path", ErrInvalidQuery)
	}
	info, err := s.resolvePath(observability.WithInternal(ctx), id, path, 0)
	if err != nil {
		return FileInfo{}, err
	}
	if info.Kind != "" {
		return FileInfo{}, fmt.Errorf("%w: %q", ErrNotFile, path)
	}
	return info, nil
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
	if err := s.requireOpen(ctx); err != nil {
		return nil, err
	}
	info, err := s.resolveFile(ctx, id, path)
	if err != nil {
		return nil, err
	}
	source, err := s.blobs.Open(ctx, info.Content)
	if err != nil {
		return nil, err
	}
	return &fileReader{source: source, span: span}, nil
}

// OpenSeekable opens a file for Seek and ReadAt without copying it. Content is
// verified in full before this returns, at the cost of reading it once per open;
// after that, reads are plain file reads and are not re-verified. A corrupt file
// fails the open with cas.ErrIntegrity. A backend without the capability returns
// cas.ErrCapability; the spool adapter is the fallback.
func (s *Service) OpenSeekable(ctx context.Context, id ID, path string) (file cas.SeekableFile, resultErr error) {
	ctx, span := start(ctx, "open_seekable", string(id))
	defer func() { span.End(resultErr) }()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := s.requireOpen(ctx); err != nil {
		return nil, err
	}
	seeker, ok := s.blobs.(cas.Seeker)
	if !ok {
		return nil, cas.ErrCapability
	}
	info, err := s.resolveFile(ctx, id, path)
	if err != nil {
		return nil, err
	}
	return seeker.OpenSeekable(ctx, info.Content)
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
