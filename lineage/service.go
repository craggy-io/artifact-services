package lineage

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	"github.com/craggy-io/artifact-services/artifacts"
	"github.com/craggy-io/artifact-services/internal/canon"
	"github.com/craggy-io/artifact-services/internal/journal"
	"github.com/craggy-io/artifact-services/internal/pager"
	"github.com/craggy-io/artifact-services/observability"
	"github.com/craggy-io/artifact-services/provenance"
)

type persisted struct {
	Record   Record                 `json:"record"`
	Category observability.Category `json:"category"`
}
type Service struct {
	artifacts  artifacts.Store
	options    Options
	journal    *journal.Store
	mu         sync.RWMutex
	records    map[ID]persisted
	closed     bool
	closeDone  bool
	pages      *pager.Cache[Record]
	traversals *pager.Cache[traversalItem]
}

var _ Store = (*Service)(nil)

func New(store artifacts.Store, options Options) (*Service, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: nil artifact store", ErrInvalid)
	}
	j, err := journal.Open(options.Root, journal.Options{Config: []byte(`{"version":1,"mapping":"ndjson-v1"}`)})
	if err != nil {
		return nil, err
	}
	s := &Service{artifacts: store, options: options, journal: j, records: map[ID]persisted{}, pages: pager.New[Record](), traversals: pager.New[traversalItem]()}
	entries, _ := j.Snapshot()
	for _, entry := range entries {
		var p persisted
		if err := json.Unmarshal(entry.Data, &p); err != nil {
			s.Close()
			return nil, err
		}
		if string(p.Record.ID) != entry.Key || p.Category > observability.Internal {
			s.Close()
			return nil, fmt.Errorf("%w: corrupt journal envelope", ErrInvalid)
		}
		s.records[p.Record.ID] = p
	}
	return s, nil
}

// Close stops operations and releases the root lease. If lease release fails,
// a later Close retries it instead of reporting success.
func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closeDone {
		return nil
	}
	s.closed = true
	if err := s.journal.Close(); err != nil {
		return err
	}
	s.closeDone = true
	return nil
}

func closedError() error { return errors.Join(ErrClosed, journal.ErrClosed) }
func start(ctx context.Context, operation, key string) (context.Context, *observability.Span) {
	return observability.Start(ctx, observability.Scope{Layer: "lineage", Operation: operation}, key)
}
func cloneRecord(r Record) Record {
	b, _ := json.Marshal(r)
	var out Record
	_ = json.Unmarshal(b, &out)
	return out
}
func validID(id ID) bool {
	if len(id) == 0 || len(id) > 256 || strings.TrimSpace(string(id)) != string(id) {
		return false
	}
	for _, r := range id {
		if r < 32 || r == 127 {
			return false
		}
	}
	return true
}
func kind(r Record) string {
	if r.Activity != nil {
		return "activity"
	}
	if r.Relationship != nil {
		return "relationship"
	}
	if r.Revision != nil {
		return "revision"
	}
	return ""
}
func (s *Service) requireOpen() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return closedError()
	}
	return nil
}
func (s *Service) lookup(id ID) (persisted, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.records[id]
	return p, ok && !s.closed
}
func (s *Service) Record(ctx context.Context, r Record) (id ID, result error) {
	category := observability.CategoryFrom(ctx)
	ctx, span := start(ctx, "record", string(r.ID))
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
			id = r.ID
			var stage *RecordError
			if !errors.As(result, &stage) {
				result = &RecordError{ID: r.ID, Committed: committed, Err: result}
			}
		}
		span.End(result)
	}()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := s.requireOpen(); err != nil {
		return "", err
	}
	encoded, err := canon.Marshal(r)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if len(encoded) > 1<<20 {
		return "", ErrLimit
	}
	if err = json.Unmarshal(encoded, &r); err != nil {
		return "", err
	}
	if err = s.validateRecord(observability.WithInternal(ctx), r); err != nil {
		return "", err
	}
	envelope, err := canon.Marshal(persisted{Record: r, Category: category})
	if err != nil {
		return "", err
	}
	// Append (including its file sync) runs outside the service lock; the
	// journal's own mutex and idempotent/conflict semantics serialize writers.
	if err := s.requireOpen(); err != nil {
		return "", err
	}
	var entry journal.Entry
	entry, reused, err = s.journal.Append(ctx, string(r.ID), envelope)
	committed = err == nil
	var commit *journal.CommitError
	if errors.As(err, &commit) {
		committed = commit.Committed
	}
	if committed {
		var stored persisted
		if e := json.Unmarshal(entry.Data, &stored); e != nil {
			return r.ID, &RecordError{ID: r.ID, Committed: true, Err: e}
		}
		s.mu.Lock()
		if !s.closed {
			s.records[r.ID] = stored
		}
		s.mu.Unlock()
	}
	if err != nil {
		switch {
		case errors.Is(err, journal.ErrConflict):
			err = errors.Join(ErrConflict, err)
		case errors.Is(err, journal.ErrCapacity):
			err = errors.Join(ErrLimit, err)
		case errors.Is(err, journal.ErrClosed):
			err = errors.Join(closedError(), err)
		}
		return r.ID, &RecordError{ID: r.ID, Committed: committed, Err: err}
	}
	return r.ID, nil
}
func (s *Service) Get(ctx context.Context, id ID) (r Record, result error) {
	ctx, span := start(ctx, "get", string(id))
	defer func() { span.End(result) }()
	if err := ctx.Err(); err != nil {
		return r, err
	}
	if err := s.requireOpen(); err != nil {
		return r, err
	}
	if !validID(id) {
		return r, ErrInvalid
	}
	p, ok := s.lookup(id)
	if !ok {
		return r, ErrNotFound
	}
	return cloneRecord(p.Record), nil
}
func (s *Service) validateRef(ctx context.Context, ref provenance.Ref) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ref.Validate(); err != nil {
		return errors.Join(ErrInvalid, err)
	}
	if ref.Revision != "" {
		p, ok := s.lookup(ID(ref.Revision))
		if !ok || p.Record.Revision == nil {
			return fmt.Errorf("%w: revision %s", ErrNotFound, ref.Revision)
		}
		if ref.Artifact != "" && string(p.Record.Revision.Artifact) != ref.Artifact {
			return fmt.Errorf("%w: revision/artifact mismatch", ErrInvalid)
		}
	}
	for _, id := range []string{ref.Artifact, ref.Schema} {
		if id != "" {
			if _, err := s.artifacts.Category(ctx, artifacts.ID(id)); err != nil {
				return err
			}
		}
	}
	if ref.Detailed() {
		if s.options.ValidateSelector == nil {
			return ErrUnsupportedSelector
		}
		if err := s.options.ValidateSelector(ctx, ref); err != nil {
			return err
		}
	}
	return nil
}
func (s *Service) validateActivity(id ID) error {
	if id == "" {
		return nil
	}
	p, ok := s.lookup(id)
	if !ok || p.Record.Activity == nil {
		return fmt.Errorf("%w: establishing activity", ErrNotFound)
	}
	return nil
}
func (s *Service) validateRecord(ctx context.Context, r Record) error {
	if !validID(r.ID) {
		return ErrInvalid
	}
	count := 0
	for _, v := range []bool{r.Activity != nil, r.Relationship != nil, r.Revision != nil} {
		if v {
			count++
		}
	}
	if count != 1 {
		return fmt.Errorf("%w: exactly one payload required", ErrInvalid)
	}
	var refs []provenance.Ref
	if a := r.Activity; a != nil {
		if a.Kind == "" || len(a.Kind) > 128 || len(a.Inputs)+len(a.Outputs) > 10000 {
			return ErrInvalid
		}
		refs = append(refs, a.Inputs...)
		refs = append(refs, a.Outputs...)
	}
	if e := r.Relationship; e != nil {
		if e.Kind == "" || e.Kind == "revision-of" || len(e.Kind) > 128 {
			return ErrInvalid
		}
		refs = append(refs, e.From, e.To)
		if err := s.validateActivity(e.Activity); err != nil {
			return err
		}
	}
	if v := r.Revision; v != nil {
		if len(v.Parents) > 1000 {
			return ErrLimit
		}
		refs = append(refs, provenance.Ref{Artifact: string(v.Artifact)})
		seen := map[ID]bool{}
		for _, id := range v.Parents {
			if id == r.ID || seen[id] {
				return ErrInvalid
			}
			seen[id] = true
			p, ok := s.lookup(id)
			if !ok || p.Record.Revision == nil {
				return fmt.Errorf("%w: parent revision", ErrNotFound)
			}
		}
		if err := s.validateActivity(v.Activity); err != nil {
			return err
		}
	}
	for _, ref := range refs {
		if err := s.validateRef(ctx, ref); err != nil {
			return err
		}
	}
	return s.eachMapping(ctx, r, func(_ int, _ Evidence, m provenance.Mapping) error {
		if err := s.validateRef(ctx, m.Output); err != nil {
			return err
		}
		for _, d := range m.Inputs {
			if err := s.validateRef(ctx, d.Ref); err != nil {
				return err
			}
		}
		return nil
	})
}
func evidence(r Record) []Evidence {
	if r.Activity != nil {
		return r.Activity.Evidence
	}
	if r.Relationship != nil {
		return r.Relationship.Evidence
	}
	return nil
}

// mappingBudget bounds aggregate evidence decoding across many records so a
// traversal/list cannot re-decode shared evidence without limit.
type mappingBudget struct{ bytes int64 }

const maxAggregateEvidenceBytes = 32 << 20

func (b *mappingBudget) add(n int64) error {
	if b == nil {
		return nil
	}
	b.bytes += n
	if b.bytes > maxAggregateEvidenceBytes {
		return ErrLimit
	}
	return nil
}

// eachMapping streams bounded evidence records. No entire evidence payload is held
// in memory; expansion has a hard edge budget independent of caller page size.
func (s *Service) eachMapping(ctx context.Context, r Record, visit func(int, Evidence, provenance.Mapping) error) error {
	return s.eachMappingBudget(ctx, r, nil, visit)
}

func (s *Service) eachMappingBudget(ctx context.Context, r Record, budget *mappingBudget, visit func(int, Evidence, provenance.Mapping) error) error {
	refs := evidence(r)
	if len(refs) > 64 {
		return ErrLimit
	}
	index, edges := 0, 0
	var total int64
	for _, e := range refs {
		if err := e.Coverage.Validate(); err != nil {
			return errors.Join(ErrInvalid, err)
		}
		if e.Path == "" {
			return ErrInvalid
		}
		reader, err := s.artifacts.OpenFile(observability.WithInternal(ctx), e.Artifact, e.Path)
		if err != nil {
			return err
		}
		err = func() error {
			scanner := bufio.NewScanner(io.LimitReader(reader, 16<<20+1))
			scanner.Buffer(make([]byte, 4096), 1<<20)
			for scanner.Scan() {
				if err := ctx.Err(); err != nil {
					return err
				}
				line := scanner.Bytes()
				total += int64(len(line) + 1)
				if total > 16<<20 {
					return ErrLimit
				}
				if err := budget.add(int64(len(line) + 1)); err != nil {
					return err
				}
				if len(bytes.TrimSpace(line)) == 0 {
					continue
				}
				if _, err := canon.Normalize(line); err != nil {
					return errors.Join(ErrInvalid, err)
				}
				var m provenance.Mapping
				d := json.NewDecoder(bytes.NewReader(line))
				d.DisallowUnknownFields()
				if err := d.Decode(&m); err != nil {
					return errors.Join(ErrInvalid, err)
				}
				if err := m.Validate(); err != nil {
					return errors.Join(ErrInvalid, err)
				}
				edges += len(m.Inputs)
				if edges > 20000 {
					return ErrLimit
				}
				index++
				if index > 20000 {
					return ErrLimit
				}
				if err := visit(index, e, m); err != nil {
					return err
				}
			}
			if err := scanner.Err(); err != nil {
				if errors.Is(err, bufio.ErrTooLong) {
					return errors.Join(ErrLimit, err)
				}
				return err
			}
			return nil
		}()
		closeErr := reader.Close()
		if err != nil || closeErr != nil {
			return errors.Join(err, closeErr)
		}
	}
	return nil
}
func (s *Service) snapshot() ([]persisted, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, closedError()
	}
	values := make([]persisted, 0, len(s.records))
	for _, p := range s.records {
		values = append(values, p)
	}
	sort.Slice(values, func(i, j int) bool { return values[i].Record.ID < values[j].Record.ID })
	return values, nil
}
