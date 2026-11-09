package assertions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/craggy-io/artifact-services/artifacts"
	"github.com/craggy-io/artifact-services/internal/journal"
	"github.com/craggy-io/artifact-services/internal/pager"
	"github.com/craggy-io/artifact-services/observability"
	"github.com/craggy-io/artifact-services/provenance"
)

// decode reads a committed assertion and derives its state as of a sequence, where
// zero means now. Later endings do not count at an earlier sequence.
func (s *Service) decode(entry journal.Entry, asOf uint64) (Assertion, error) {
	var a Assertion
	if err := json.Unmarshal(entry.Data, &a); err != nil {
		return a, err
	}
	a.Sequence = entry.Sequence
	s.mu.RLock()
	end, ended := s.index.endings[entry.Sequence]
	s.mu.RUnlock()
	if ended && (asOf == 0 || end.sequence <= asOf) {
		a.EndedAt, a.EndedBy, a.Retracted = end.sequence, end.by, end.retracted
	}
	a.Current = a.EndedAt == 0
	return a, nil
}

// Get returns an assertion by ID whatever its state, with that state filled in.
func (s *Service) Get(ctx context.Context, id ID) (a Assertion, result error) {
	ctx, span := start(ctx, "get", string(id))
	defer func() { span.End(result) }()
	if err := ctx.Err(); err != nil {
		return a, err
	}
	if err := s.requireOpen(ctx); err != nil {
		return a, err
	}
	entry, ok, err := s.journal.Get(assertionPrefix + string(id))
	if err != nil {
		return a, err
	}
	if !ok {
		return a, fmt.Errorf("%w: assertion %s", ErrNotFound, id)
	}
	return s.decode(entry, 0)
}

// Current returns the current assertion of a keyed kind for a key, as an index hit.
// ErrNotFound means none: never recorded, or its last holder was retracted.
func (s *Service) Current(ctx context.Context, kind KindID, key string) (a Assertion, result error) {
	ctx, span := start(ctx, "current", string(kind))
	defer func() { span.End(result) }()
	if err := ctx.Err(); err != nil {
		return a, err
	}
	if err := s.requireOpen(ctx); err != nil {
		return a, err
	}
	s.mu.RLock()
	list := s.index.byKey[keyPosting(kind, key)]
	last := uint64(0)
	if len(list) > 0 {
		last = list[len(list)-1]
	}
	_, ended := s.index.endings[last]
	s.mu.RUnlock()
	if last == 0 || ended {
		return a, fmt.Errorf("%w: kind %s key %q", ErrNotFound, kind, key)
	}
	entry, err := s.journal.At(last)
	if err != nil {
		return a, err
	}
	return s.decode(entry, 0)
}

// History returns an assertion's replacement chain, oldest first, including any
// retraction state on its last member. It accepts the ID of any member.
func (s *Service) History(ctx context.Context, id ID) (chain []Assertion, result error) {
	ctx, span := start(ctx, "history", string(id))
	defer func() { span.End(result) }()
	current, err := s.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	for current.EndedBy != "" {
		if current, err = s.Get(ctx, current.EndedBy); err != nil {
			return nil, err
		}
	}
	chain = []Assertion{current}
	for current.Replaces != "" {
		if current, err = s.Get(ctx, current.Replaces); err != nil {
			return nil, err
		}
		chain = append(chain, current)
	}
	for i, j := 0, len(chain)-1; i < j; i, j = i+1, j-1 {
		chain[i], chain[j] = chain[j], chain[i]
	}
	return chain, nil
}

// intersect merges ascending lists to those present in all of them.
func intersect(lists [][]uint64) []uint64 {
	if len(lists) == 0 {
		return nil
	}
	result := lists[0]
	for _, list := range lists[1:] {
		merged := make([]uint64, 0, min(len(result), len(list)))
		i, j := 0, 0
		for i < len(result) && j < len(list) {
			switch {
			case result[i] < list[j]:
				i++
			case result[i] > list[j]:
				j++
			default:
				merged = append(merged, result[i])
				i++
				j++
			}
		}
		result = merged
	}
	return result
}

// unite merges two ascending lists without duplicates.
func unite(a, b []uint64) []uint64 {
	merged := make([]uint64, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) || j < len(b) {
		switch {
		case j == len(b) || i < len(a) && a[i] < b[j]:
			merged = append(merged, a[i])
			i++
		case i == len(a) || b[j] < a[i]:
			merged = append(merged, b[j])
			j++
		default:
			merged = append(merged, a[i])
			i++
			j++
		}
	}
	return merged
}

// contentOf returns the content digest assertions on an occurrence should also match.
func (s *Service) contentOf(ctx context.Context, subject provenance.Ref) (string, error) {
	if subject.Occurrence == "" {
		return "", fmt.Errorf("%w: IncludeContent needs an occurrence subject", ErrInvalid)
	}
	occurrence, err := s.artifacts.GetOccurrence(observability.WithInternal(ctx), artifacts.OccurrenceID(subject.Occurrence))
	if err != nil {
		return "", err
	}
	return string(occurrence.Content), nil
}

// matching answers from resident posting lists without touching the log.
func (s *Service) matching(q Query, contentSubject string) []uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var lists [][]uint64
	if q.Kind != "" && q.Key == "" {
		lists = append(lists, s.index.byKind.Snapshot(string(q.Kind)))
	}
	if q.Key != "" {
		lists = append(lists, s.index.byKey.Snapshot(keyPosting(q.Kind, q.Key)))
	}
	if q.Subject != nil {
		list := s.index.bySubject.Snapshot(refKey(*q.Subject))
		if contentSubject != "" {
			list = unite(list, s.index.bySubject.Snapshot(refKey(provenance.Ref{Digest: contentSubject})))
		}
		lists = append(lists, list)
	}
	if q.Evidence != nil {
		lists = append(lists, s.index.byCited.Snapshot(refKey(*q.Evidence)))
	}
	candidates := s.index.all[:len(s.index.all):len(s.index.all)]
	if len(lists) > 0 {
		candidates = intersect(lists)
	}
	result := make([]uint64, 0, len(candidates))
	for _, sequence := range candidates {
		if q.AsOf != 0 && sequence > q.AsOf {
			break
		}
		if s.index.internal[sequence] && !q.IncludeInternal {
			continue
		}
		if !q.IncludeEnded {
			if end, ended := s.index.endings[sequence]; ended && (q.AsOf == 0 || end.sequence <= q.AsOf) {
				continue
			}
		}
		result = append(result, sequence)
	}
	return result
}

// List answers a query from indexes: its work is the matching records, not the size
// of the journal. Decoding happens only for the page returned.
func (s *Service) List(ctx context.Context, q Query) (page Page, result error) {
	ctx, span := start(ctx, "list", string(q.Kind))
	defer func() { span.End(result) }()
	if err := ctx.Err(); err != nil {
		return page, err
	}
	if err := s.requireOpen(ctx); err != nil {
		return page, err
	}
	if q.Key != "" && q.Kind == "" {
		return page, fmt.Errorf("%w: key requires kind", ErrInvalid)
	}
	cursor := q.Cursor
	key := q
	key.Cursor = ""
	sequences, next, err := s.pages.Page(key, cursor, q.Limit, func() ([]uint64, error) {
		content := ""
		if q.IncludeContent {
			if q.Subject == nil {
				return nil, fmt.Errorf("%w: IncludeContent needs a subject", ErrInvalid)
			}
			var err error
			if content, err = s.contentOf(ctx, *q.Subject); err != nil {
				return nil, err
			}
		}
		return s.matching(q, content), nil
	})
	if err != nil {
		switch {
		case errors.Is(err, pager.ErrCursor):
			return page, errors.Join(ErrCursor, err)
		case errors.Is(err, pager.ErrCapacity):
			return page, errors.Join(ErrLimit, err)
		}
		return page, err
	}
	assertions := make([]Assertion, 0, len(sequences))
	for _, sequence := range sequences {
		entry, err := s.journal.At(sequence)
		if err != nil {
			return page, err
		}
		a, err := s.decode(entry, q.AsOf)
		if err != nil {
			return page, err
		}
		assertions = append(assertions, a)
	}
	span.Count("returned", int64(len(assertions)))
	return Page{Assertions: assertions, NextCursor: next}, nil
}
