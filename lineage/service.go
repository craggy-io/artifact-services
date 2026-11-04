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
	"sync"

	"github.com/craggy-io/artifact-services/artifacts"
	"github.com/craggy-io/artifact-services/cas"
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

// Service keeps records in the journal, reading them back on demand. Only head
// pointers and posting lists are resident. Lock order is the journal's lock, then
// mu: Commit runs hooks holding the former that take the latter.
type Service struct {
	artifacts  artifacts.Store
	options    Options
	journal    *journal.Store
	indexed    map[string]bool
	resolver   provenance.Resolver
	mu         sync.RWMutex
	heads      map[string]ID
	histories  journal.Postings
	metadata   journal.Postings
	closed     bool
	closeDone  bool
	pages      *pager.Cache[uint64]
	traversals *pager.Cache[traversalItem]
}

var _ Store = (*Service)(nil)

func New(store artifacts.Store, options Options) (*Service, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: nil artifact store", ErrInvalid)
	}
	keys := append([]string(nil), options.IndexedMetadata...)
	sort.Strings(keys)
	if len(keys) > 16 {
		return nil, fmt.Errorf("%w: indexed-key limit", ErrInvalid)
	}
	indexed := map[string]bool{}
	for _, key := range keys {
		if key == "" || len(key) > 256 || indexed[key] {
			return nil, fmt.Errorf("%w: indexed key", ErrInvalid)
		}
		indexed[key] = true
	}
	config, err := canon.Marshal(struct {
		Version int      `json:"version"`
		Mapping string   `json:"mapping"`
		Indexed []string `json:"indexed"`
	}{1, "ndjson-v1", keys})
	if err != nil {
		return nil, err
	}
	j, err := journal.Open(options.Root, journal.Options{Config: config, MaxRecords: options.MaxRecords, MaxBytes: options.MaxBytes, Relaxed: options.Relaxed})
	if err != nil {
		return nil, err
	}
	s := &Service{artifacts: store, options: options, journal: j, indexed: indexed, heads: map[string]ID{}, histories: journal.Postings{}, metadata: journal.Postings{}, pages: pager.New[uint64](), traversals: pager.New[traversalItem]()}
	s.resolver = provenance.Resolver{
		Revision: s.ValidateRevision,
		Artifact: func(ctx context.Context, id string) error {
			_, err := store.Category(ctx, artifacts.ID(id))
			return err
		},
		Occurrence: func(ctx context.Context, id string) (string, error) {
			o, err := store.GetOccurrence(ctx, artifacts.OccurrenceID(id))
			return string(o.Artifact), err
		},
		Content: func(ctx context.Context, digest string) (int64, error) { return store.StatContent(ctx, cas.ID(digest)) },
		File: func(ctx context.Context, artifact, path string) (int64, error) {
			info, err := store.Stat(ctx, artifacts.ID(artifact), path)
			if err != nil {
				return 0, err
			}
			if info.Kind != "" {
				return -1, nil
			}
			return info.Size, nil
		},
		Selector: func(ctx context.Context, ref provenance.Ref) error {
			if options.ValidateSelector == nil {
				return ErrUnsupportedSelector
			}
			return options.ValidateSelector(ctx, ref)
		},
		Assertion: options.ValidateAssertion,
		Activity: func(ctx context.Context, id string) error {
			return s.validateActivity(ID(id))
		},
	}
	err = j.Scan(context.Background(), 0, 0, func(entry journal.Entry) error {
		p, err := decodePersisted(entry)
		if err != nil {
			return err
		}
		s.applyIndexes(p.Record, entry.Sequence)
		return nil
	})
	if err != nil {
		return nil, errors.Join(err, j.Close())
	}
	return s, nil
}

func decodePersisted(entry journal.Entry) (persisted, error) {
	var p persisted
	if err := json.Unmarshal(entry.Data, &p); err != nil {
		return persisted{}, err
	}
	if string(p.Record.ID) != entry.Key || p.Category > observability.Internal {
		return persisted{}, fmt.Errorf("%w: corrupt journal envelope", ErrInvalid)
	}
	p.Record.Sequence = entry.Sequence
	return p, nil
}

// applyIndexes updates resident state for a committed record. Callers hold mu or
// have exclusive access during open.
func (s *Service) applyIndexes(r Record, sequence uint64) {
	if v := r.Revision; v != nil && v.History != "" {
		s.heads[v.History] = r.ID
		s.histories.Add(v.History, sequence)
	}
	for key, value := range recordMetadata(r) {
		if s.indexed[key] {
			if normalized, err := canon.IndexedScalar(value); err == nil {
				s.metadata.Add(journal.IndexKey(key, string(normalized)), sequence)
			}
		}
	}
}

// Close stops operations and releases the root lease. If lease release fails,
// a later Close retries it instead of reporting success.
func (s *Service) Close() error {
	s.mu.Lock()
	if s.closeDone {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	// Not under mu: a commit in flight holds the journal lock and wants mu.
	if err := s.journal.Close(); err != nil {
		return err
	}
	s.mu.Lock()
	s.closeDone = true
	s.mu.Unlock()
	return nil
}

// Sequence returns the journal's latest commit sequence. It increases by exactly
// one per new record and survives restart.
func (s *Service) Sequence(ctx context.Context) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := s.requireOpen(); err != nil {
		return 0, err
	}
	return s.journal.Sequence(), nil
}

// OnCommit registers a hook fired after each durable commit with the newest
// sequence. Calls are serial, may skip sequences, and never block a commit: on any
// wake, read Changes from your last position. The stop function may be called by
// the hook itself. Hooks end when the service closes.
func (s *Service) OnCommit(hook func(sequence uint64)) (stop func()) {
	return s.journal.OnCommit(hook)
}

// Changes reads committed records in sequence order from a stored position. Class
// is the record's kind. Kind is the activity or relationship kind, or a revision's
// history. Activity is a revision's or relationship's establishing activity.
func (s *Service) Changes(ctx context.Context, q provenance.ChangeQuery) (page provenance.ChangePage, result error) {
	ctx, span := start(ctx, "changes", "")
	defer func() { span.End(result) }()
	if err := s.requireOpen(); err != nil {
		return page, err
	}
	return s.journal.Changes(ctx, q, func(entry journal.Entry) (provenance.Change, error) {
		p, err := decodePersisted(entry)
		if err != nil {
			return provenance.Change{}, err
		}
		r := p.Record
		change := provenance.Change{Class: kind(r), ID: string(r.ID), Internal: p.Category == observability.Internal}
		switch {
		case r.Activity != nil:
			change.Kind, change.Actor = r.Activity.Kind, r.Activity.Actor
		case r.Relationship != nil:
			change.Kind, change.Activity = r.Relationship.Kind, string(r.Relationship.Activity)
		case r.Revision != nil:
			change.Kind, change.Activity = r.Revision.History, string(r.Revision.Activity)
		}
		return change, nil
	})
}

func closedError() error { return errors.Join(ErrClosed, journal.ErrClosed) }
func start(ctx context.Context, operation, key string) (context.Context, *observability.Span) {
	return observability.Start(ctx, observability.Scope{Layer: "lineage", Operation: operation}, key)
}
func validID(id ID) bool { return provenance.ValidRecordID(string(id)) }
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

// lookup reads a record from the journal. A read failure is reported as absent to
// validation, which is the safe direction: nothing gets stored against it.
func (s *Service) lookup(id ID) (persisted, bool) {
	entry, ok, err := s.journal.Get(string(id))
	if err != nil || !ok {
		return persisted{}, false
	}
	p, err := decodePersisted(entry)
	return p, err == nil
}

func (s *Service) normalizePredicates(metadata map[string]json.RawMessage) (map[string]json.RawMessage, error) {
	if len(metadata) == 0 {
		return nil, nil
	}
	if len(metadata) > 16 {
		return nil, fmt.Errorf("%w: predicate count", ErrInvalid)
	}
	predicates := map[string]json.RawMessage{}
	for key, value := range metadata {
		if !s.indexed[key] {
			return nil, fmt.Errorf("%w: unindexed metadata key %q", ErrInvalid, key)
		}
		normalized, err := canon.IndexedScalar(value)
		if err != nil {
			return nil, fmt.Errorf("%w: metadata %q: %w", ErrInvalid, key, err)
		}
		predicates[key] = normalized
	}
	return predicates, nil
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
	r.Sequence = 0
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
	if err := s.requireOpen(); err != nil {
		return "", err
	}
	// The head check and the record are one commit, so racing writers on a guarded
	// history cannot both pass it. A replay of the same record reuses without being
	// re-checked, so a retry succeeds even after the head has moved on.
	txn := journal.Txn{Key: string(r.ID), Data: envelope, Check: func() error { return s.checkHead(r) }}
	txn.Apply = func(entry journal.Entry) {
		s.mu.Lock()
		s.applyIndexes(r, entry.Sequence)
		s.mu.Unlock()
	}
	_, reused, err = s.journal.Commit(ctx, txn)
	committed = err == nil
	if err != nil {
		switch {
		case errors.Is(err, journal.ErrConflict):
			err = errors.Join(ErrConflict, err)
		case errors.Is(err, journal.ErrCapacity):
			err = errors.Join(ErrLimit, err)
		case errors.Is(err, journal.ErrClosed):
			err = errors.Join(closedError(), err)
		}
		return r.ID, &RecordError{ID: r.ID, Committed: false, Err: err}
	}
	return r.ID, nil
}

// checkHead runs inside the commit critical section.
func (s *Service) checkHead(r Record) error {
	v := r.Revision
	if v == nil || v.History == "" {
		return nil
	}
	s.mu.RLock()
	head := s.heads[v.History]
	s.mu.RUnlock()
	var expected ID
	if len(v.Parents) == 1 {
		expected = v.Parents[0]
	}
	if len(v.Parents) > 1 || head != expected {
		return &HeadConflictError{History: v.History, Expected: expected, Actual: head}
	}
	return nil
}

// AppendRevision is one guarded step of a linear history. Expected names the head
// this revision was built on; empty means the history must not exist yet.
type AppendRevision struct {
	ID       ID
	History  string
	Expected ID
	Artifact artifacts.ID
	Activity ID
	Metadata map[string]json.RawMessage
}

// Append records a revision only if the history's head is still Expected. Exactly
// one of two racing writers wins; the other gets a RecordError matching
// ErrHeadConflict and carrying HeadConflictError, and nothing was committed.
func (s *Service) Append(ctx context.Context, a AppendRevision) (ID, error) {
	revision := &Revision{Artifact: a.Artifact, History: a.History, Activity: a.Activity, Metadata: a.Metadata}
	if a.Expected != "" {
		revision.Parents = []ID{a.Expected}
	}
	return s.Record(ctx, Record{ID: a.ID, Revision: revision})
}

// Head returns a guarded history's tip. ErrNotFound means no such history.
func (s *Service) Head(ctx context.Context, history string) (id ID, result error) {
	ctx, span := start(ctx, "head", history)
	defer func() { span.End(result) }()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := s.requireOpen(); err != nil {
		return "", err
	}
	s.mu.RLock()
	head, ok := s.heads[history]
	s.mu.RUnlock()
	if !ok {
		return "", ErrNotFound
	}
	return head, nil
}

// HeadAt returns a guarded history's tip as of a journal sequence. It walks back
// from the current head, so its cost is the revisions committed since then.
func (s *Service) HeadAt(ctx context.Context, history string, sequence uint64) (ID, error) {
	id, err := s.Head(ctx, history)
	if err != nil {
		return "", err
	}
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		p, ok := s.lookup(id)
		if !ok || p.Record.Revision == nil {
			return "", fmt.Errorf("%w: revision %s", ErrNotFound, id)
		}
		if p.Record.Sequence <= sequence {
			return id, nil
		}
		if len(p.Record.Revision.Parents) == 0 {
			return "", ErrNotFound
		}
		id = p.Record.Revision.Parents[0]
	}
}

func cloneRecord(r Record) Record {
	b, _ := json.Marshal(r)
	var out Record
	_ = json.Unmarshal(b, &out)
	return out
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
	entry, ok, err := s.journal.Get(string(id))
	if err != nil {
		return r, err
	}
	if !ok {
		return r, ErrNotFound
	}
	p, err := decodePersisted(entry)
	if err != nil {
		return r, err
	}
	return cloneRecord(p.Record), nil
}

// ValidateRevision reports whether a revision exists and, if an artifact is given,
// pins it. Wire it into another layer's options to check references to revisions.
func (s *Service) ValidateRevision(_ context.Context, revision, artifact string) error {
	p, ok := s.lookup(ID(revision))
	if !ok || p.Record.Revision == nil {
		return fmt.Errorf("%w: revision %s", ErrNotFound, revision)
	}
	if artifact != "" && string(p.Record.Revision.Artifact) != artifact {
		return fmt.Errorf("%w: revision/artifact mismatch", ErrInvalid)
	}
	return nil
}

func (s *Service) validateRef(ctx context.Context, ref provenance.Ref) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ref.Validate(); err != nil {
		return errors.Join(ErrInvalid, err)
	}
	return s.resolver.Check(ctx, ref)
}

// ValidateActivity reports whether an activity exists. Wire it into another layer's
// options to check references to activities.
func (s *Service) ValidateActivity(_ context.Context, id string) error {
	return s.validateActivity(ID(id))
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
		if v.History != "" && (!provenance.ValidRecordID(v.History) || len(v.Parents) > 1) {
			return fmt.Errorf("%w: guarded history needs a valid name and one parent", ErrInvalid)
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
	for key, value := range recordMetadata(r) {
		if s.indexed[key] {
			if _, err := canon.IndexedScalar(value); err != nil {
				return fmt.Errorf("%w: indexed metadata %q: %w", ErrInvalid, key, err)
			}
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
