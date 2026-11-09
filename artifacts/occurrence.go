package artifacts

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/craggy-io/artifact-services/cas"
	"github.com/craggy-io/artifact-services/internal/canon"
	"github.com/craggy-io/artifact-services/internal/journal"
	"github.com/craggy-io/artifact-services/observability"
	"github.com/craggy-io/artifact-services/provenance"
)

const occurrenceKeyPrefix = "occurrence/"

// OccurrenceID identifies one publication of a snapshot. A caller-chosen ID makes
// recording it idempotent, which is what a retried composite operation relies on.
type OccurrenceID string

// OccurrenceRequest records that a snapshot was published as a separately
// describable thing. None of its fields are hash inputs, so identical bytes under
// different names, origins or producers are distinct occurrences of equal content.
// An empty ID mints a random one. Name, Origin and Producer are opaque to this layer.
type OccurrenceRequest struct {
	ID       OccurrenceID
	Artifact ID
	Name     string
	Origin   string
	Producer string
	Metadata map[string]json.RawMessage
}

// Occurrence is an immutable publication record.
type Occurrence struct {
	ID       OccurrenceID               `json:"id"`
	Artifact ID                         `json:"artifact"`
	Content  cas.ID                     `json:"content"`
	Name     string                     `json:"name,omitempty"`
	Origin   string                     `json:"origin,omitempty"`
	Producer string                     `json:"producer,omitempty"`
	Metadata map[string]json.RawMessage `json:"metadata,omitempty"`
	Category observability.Category     `json:"category"`
	Sequence uint64                     `json:"sequence,omitempty"`

	indexed map[string]json.RawMessage
}

// OccurrenceError distinguishes a durable occurrence from a failure before commit.
type OccurrenceError struct {
	ID        OccurrenceID
	Committed bool
	Err       error
}

func (e *OccurrenceError) Error() string {
	return fmt.Sprintf("artifacts: occurrence %s (committed=%t): %v", e.ID, e.Committed, e.Err)
}
func (e *OccurrenceError) Unwrap() error { return e.Err }

// OccurrenceQuery predicates are ANDed. Content finds every occurrence of equal
// content, across differently named or wrapped snapshots; Artifact is exact.
// Metadata predicates need keys configured in Options.IndexedMetadata.
type OccurrenceQuery struct {
	Artifact        ID
	Content         cas.ID
	Name            string
	Metadata        map[string]json.RawMessage
	IncludeInternal bool
	Limit           int
	Cursor          string
}

// OccurrencePage is one page, ordered by commit sequence.
type OccurrencePage struct {
	Occurrences []Occurrence
	NextCursor  string
}

func mintOccurrenceID() (OccurrenceID, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}
	return OccurrenceID("occ-" + hex.EncodeToString(random[:])), nil
}

func (s *Service) indexedOf(metadata map[string]json.RawMessage) (map[string]json.RawMessage, error) {
	indexed := map[string]json.RawMessage{}
	for key := range s.indexed {
		if value, ok := metadata[key]; ok {
			normalized, err := canon.IndexedScalar(value)
			if err != nil {
				return nil, invalidf("metadata %s: %w", key, err)
			}
			indexed[key] = normalized
		}
	}
	return indexed, nil
}

// RecordOccurrence commits an occurrence of a registered artifact. Repeating an
// identical request with the same ID returns the original and changes nothing; the
// same ID with different fields is ErrConflict.
func (s *Service) RecordOccurrence(ctx context.Context, request OccurrenceRequest) (occurrence Occurrence, result error) {
	category := observability.CategoryFrom(ctx)
	ctx, span := start(ctx, "record_occurrence", string(request.ID))
	committed, reused := false, false
	defer func() {
		if committed {
			if reused {
				span.Count("reused", 1)
			} else {
				span.Count("recorded", 1)
			}
		}
		if result != nil {
			result = &OccurrenceError{ID: request.ID, Committed: committed, Err: result}
		}
		span.End(result)
	}()
	if err := ctx.Err(); err != nil {
		return Occurrence{}, err
	}
	if err := s.requireOpen(ctx); err != nil {
		return Occurrence{}, err
	}
	if request.ID == "" {
		minted, err := mintOccurrenceID()
		if err != nil {
			return Occurrence{}, err
		}
		request.ID = minted
	}
	if !provenance.ValidRecordID(string(request.ID)) {
		return Occurrence{}, fmt.Errorf("%w: occurrence ID", ErrInvalidQuery)
	}
	for _, field := range []string{request.Name, request.Origin, request.Producer} {
		if len(field) > 4096 {
			return Occurrence{}, fmt.Errorf("%w: occurrence field limit", ErrInvalidQuery)
		}
	}
	metadata, err := normalizeMetadata(request.Metadata, false)
	if err != nil {
		return Occurrence{}, err
	}
	member, err := s.registered(ctx, request.Artifact)
	if err != nil {
		return Occurrence{}, err
	}
	occurrence = Occurrence{ID: request.ID, Artifact: request.Artifact, Content: member.Content, Name: request.Name, Origin: request.Origin, Producer: request.Producer, Metadata: metadata, Category: category}
	if occurrence.indexed, err = s.indexedOf(metadata); err != nil {
		return Occurrence{}, err
	}
	data, err := canon.Marshal(occurrence)
	if err != nil {
		return Occurrence{}, err
	}
	entry, reused, err := s.records.Commit(ctx, journal.Txn{Key: occurrenceKeyPrefix + string(request.ID), Data: data, Apply: func(e journal.Entry) {
		s.mu.Lock()
		s.index.addOccurrence(e.Sequence, occurrence)
		s.mu.Unlock()
	}})
	if err != nil {
		switch {
		case errors.Is(err, journal.ErrConflict):
			err = errors.Join(ErrConflict, err)
		case errors.Is(err, journal.ErrCapacity):
			err = errors.Join(ErrLimit, err)
		case errors.Is(err, journal.ErrClosed):
			err = errors.Join(closedError(), err)
		}
		return Occurrence{}, err
	}
	committed = true
	return s.decodeOccurrence(entry)
}

func (s *Service) decodeOccurrence(entry journal.Entry) (Occurrence, error) {
	var o Occurrence
	if err := json.Unmarshal(entry.Data, &o); err != nil {
		return Occurrence{}, err
	}
	if occurrenceKeyPrefix+string(o.ID) != entry.Key || o.Category > observability.Internal {
		return Occurrence{}, errors.New("artifacts: corrupt occurrence")
	}
	o.Sequence = entry.Sequence
	indexed, err := s.indexedOf(o.Metadata)
	if err != nil {
		return Occurrence{}, err
	}
	o.indexed = indexed
	return o, nil
}

// GetOccurrence reads one occurrence by ID.
func (s *Service) GetOccurrence(ctx context.Context, id OccurrenceID) (occurrence Occurrence, result error) {
	ctx, span := start(ctx, "get_occurrence", string(id))
	defer func() { span.End(result) }()
	if err := ctx.Err(); err != nil {
		return Occurrence{}, err
	}
	if err := s.requireOpen(ctx); err != nil {
		return Occurrence{}, err
	}
	if !provenance.ValidRecordID(string(id)) {
		return Occurrence{}, fmt.Errorf("%w: occurrence ID", ErrInvalidQuery)
	}
	entry, ok, err := s.records.Get(occurrenceKeyPrefix + string(id))
	if err != nil {
		return Occurrence{}, err
	}
	if !ok {
		return Occurrence{}, ErrNotFound
	}
	return s.decodeOccurrence(entry)
}

// Occurrences pages occurrences through an index, oldest first. At least one of
// Artifact, Content, Name or Metadata is required: an unfiltered listing is a
// sequence read, which Changes already provides.
func (s *Service) Occurrences(ctx context.Context, q OccurrenceQuery) (page OccurrencePage, result error) {
	ctx, span := start(ctx, "occurrences", "")
	defer func() { span.End(result) }()
	if err := ctx.Err(); err != nil {
		return page, err
	}
	if err := s.requireOpen(ctx); err != nil {
		return page, err
	}
	limit, err := normalizeLimit(q.Limit)
	if err != nil {
		return page, err
	}
	q.Limit = limit
	if q.Artifact != "" {
		if _, err := ParseID(string(q.Artifact)); err != nil {
			return page, fmt.Errorf("%w: %w", ErrInvalidQuery, err)
		}
	}
	if q.Content != "" {
		if _, err := cas.ParseID(string(q.Content)); err != nil {
			return page, fmt.Errorf("%w: %w", ErrInvalidQuery, err)
		}
	}
	predicates, err := s.normalizePredicates(q.Metadata)
	if err != nil {
		return page, err
	}
	q.Metadata = predicates
	if q.Artifact == "" && q.Content == "" && q.Name == "" && len(predicates) == 0 {
		return page, fmt.Errorf("%w: occurrence query needs a predicate", ErrInvalidQuery)
	}
	cursor := q.Cursor
	q.Cursor = ""
	sequences, next, err := s.searches.Page(struct {
		Occurrences OccurrenceQuery
	}{q}, cursor, limit, func() ([]uint64, error) {
		candidates := s.occurrenceCandidates(q)
		matched := []uint64{}
		for _, sequence := range candidates {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			o, err := s.occurrenceAt(sequence)
			if err != nil {
				return nil, err
			}
			if occurrenceMatches(o, q) {
				matched = append(matched, sequence)
			}
		}
		return matched, nil
	})
	if err != nil {
		return page, mapPageError(err)
	}
	page.NextCursor = next
	page.Occurrences = make([]Occurrence, 0, len(sequences))
	for _, sequence := range sequences {
		o, err := s.occurrenceAt(sequence)
		if err != nil {
			return OccurrencePage{}, err
		}
		page.Occurrences = append(page.Occurrences, o)
	}
	span.Count("returned", int64(len(page.Occurrences)))
	return page, nil
}

func (s *Service) occurrenceAt(sequence uint64) (Occurrence, error) {
	entry, err := s.records.At(sequence)
	if err != nil {
		return Occurrence{}, err
	}
	return s.decodeOccurrence(entry)
}

// occurrenceCandidates picks the narrowest posting list; the rest are checked
// against each candidate record.
func (s *Service) occurrenceCandidates(q OccurrenceQuery) []uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var lists [][]uint64
	if q.Artifact != "" {
		lists = append(lists, s.index.occArtifact.Snapshot(string(q.Artifact)))
	}
	if q.Content != "" {
		lists = append(lists, s.index.occContent.Snapshot(string(q.Content)))
	}
	if q.Name != "" {
		lists = append(lists, s.index.occName.Snapshot(q.Name))
	}
	for key, value := range q.Metadata {
		lists = append(lists, s.index.occMetadata.Snapshot(journal.IndexKey(key, string(value))))
	}
	sort.Slice(lists, func(i, j int) bool { return len(lists[i]) < len(lists[j]) })
	return lists[0]
}

func occurrenceMatches(o Occurrence, q OccurrenceQuery) bool {
	if o.Category == observability.Internal && !q.IncludeInternal {
		return false
	}
	if q.Artifact != "" && o.Artifact != q.Artifact || q.Content != "" && o.Content != q.Content || q.Name != "" && o.Name != q.Name {
		return false
	}
	for key, value := range q.Metadata {
		if !bytes.Equal(o.indexed[key], value) {
			return false
		}
	}
	return true
}

// StatContent finds bare content by digest, wherever it occurs. It returns the byte
// size of a single stream, or -1 when the digest names a whole entry list rather
// than one stream. ErrNotFound means neither CAS nor the catalog knows the digest.
func (s *Service) StatContent(ctx context.Context, id cas.ID) (size int64, result error) {
	ctx, span := start(ctx, "stat_content", string(id))
	defer func() { span.End(result) }()
	if err := s.requireOpen(ctx); err != nil {
		return 0, err
	}
	if _, err := cas.ParseID(string(id)); err != nil {
		return 0, err
	}
	s.mu.RLock()
	registered := len(s.index.content.Snapshot(string(id))) > 0
	s.mu.RUnlock()
	exists, err := s.blobs.Exists(ctx, id)
	if err != nil {
		return 0, err
	}
	if exists {
		if statter, ok := s.blobs.(cas.Statter); ok {
			size, err := statter.Stat(ctx, id)
			if err == nil || !errors.Is(err, cas.ErrCapability) {
				return size, err
			}
		}
		return s.hashThrough(ctx, id)
	}
	if registered {
		return -1, nil
	}
	return 0, ErrNotFound
}
