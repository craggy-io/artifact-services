package assertions

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/craggy-io/artifact-services/artifacts"
	"github.com/craggy-io/artifact-services/cas"
	"github.com/craggy-io/artifact-services/internal/canon"
	"github.com/craggy-io/artifact-services/internal/journal"
	"github.com/craggy-io/artifact-services/internal/pager"
	"github.com/craggy-io/artifact-services/objects"
	"github.com/craggy-io/artifact-services/observability"
	"github.com/craggy-io/artifact-services/provenance"
)

const (
	kindPrefix       = "k/"
	assertionPrefix  = "a/"
	retractionPrefix = "r/"
	maxRecordBytes   = 1 << 20
	maxEvidence      = 256
	maxKeyBytes      = 1024
	maxWriterBytes   = 256
)

// retractionRecord is the persisted form of a Retraction.
type retractionRecord struct {
	Assertion ID                     `json:"assertion"`
	Activity  string                 `json:"activity,omitempty"`
	Actor     string                 `json:"actor,omitempty"`
	Writer    string                 `json:"writer,omitempty"`
	Category  observability.Category `json:"category,omitempty"`
}

// ending is how an assertion stopped being current.
type ending struct {
	sequence  uint64
	by        ID
	retracted bool
}

// indexes is derived entirely from the log and rebuilt on open. Lists hold
// sequences, not values, and values stay on disk. Lock order is the journal's lock,
// then mu: Commit runs Check and Apply holding the former.
type indexes struct {
	kinds     map[KindID]Kind
	byKind    journal.Postings
	bySubject journal.Postings
	byCited   journal.Postings
	byKey     journal.Postings
	all       []uint64
	endings   map[uint64]ending
	internal  map[uint64]bool
}

// Service is the assertion layer over one exclusive journal root.
type Service struct {
	artifacts artifacts.Store
	options   Options
	journal   *journal.Store
	resolver  provenance.Resolver
	mu        sync.RWMutex
	index     indexes
	closed    bool
	closeDone bool
	pages     *pager.Cache[uint64]
}

// New opens the journal, rebuilding indexes by streaming its records.
func New(store artifacts.Store, options Options) (*Service, error) {
	if store == nil {
		return nil, fmt.Errorf("%w: nil artifact store", ErrInvalid)
	}
	config, err := canon.Marshal(struct {
		Version int `json:"version"`
	}{1})
	if err != nil {
		return nil, err
	}
	j, err := journal.Open(options.Root, journal.Options{Config: config, MaxRecords: options.MaxRecords, MaxBytes: options.MaxBytes, Relaxed: options.Relaxed})
	if err != nil {
		return nil, err
	}
	s := &Service{artifacts: store, options: options, journal: j, pages: pager.New[uint64](), index: indexes{
		kinds: map[KindID]Kind{}, byKind: journal.Postings{}, bySubject: journal.Postings{}, byCited: journal.Postings{}, byKey: journal.Postings{}, endings: map[uint64]ending{}, internal: map[uint64]bool{},
	}}
	s.resolver = s.newResolver()
	err = j.Scan(context.Background(), 0, 0, func(entry journal.Entry) error { return s.replay(entry) })
	if err != nil {
		return nil, errors.Join(err, j.Close())
	}
	return s, nil
}

func (s *Service) newResolver() provenance.Resolver {
	store, options := s.artifacts, s.options
	return provenance.Resolver{
		Revision: options.ValidateRevision,
		Artifact: func(ctx context.Context, id string) error {
			_, err := store.Category(ctx, artifacts.ID(id))
			return err
		},
		Occurrence: func(ctx context.Context, id string) (string, error) {
			occurrence, err := store.GetOccurrence(ctx, artifacts.OccurrenceID(id))
			return string(occurrence.Artifact), err
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
		Selector:  options.ValidateSelector,
		Assertion: s.ValidateAssertion,
		Activity:  options.ValidateActivity,
	}
}

// replay applies one committed record to the indexes, on open and after commit alike.
func (s *Service) replay(entry journal.Entry) error {
	switch {
	case strings.HasPrefix(entry.Key, kindPrefix):
		var k Kind
		if err := json.Unmarshal(entry.Data, &k); err != nil {
			return err
		}
		s.index.kinds[k.ID()] = k
	case strings.HasPrefix(entry.Key, assertionPrefix):
		var a Assertion
		if err := json.Unmarshal(entry.Data, &a); err != nil {
			return err
		}
		s.indexAssertion(a, entry.Sequence)
		if a.Replaces == "" {
			return nil
		}
		target, ok, err := s.journal.Get(assertionPrefix + string(a.Replaces))
		if err != nil || !ok {
			return errors.Join(fmt.Errorf("%w: replaced assertion missing from journal", ErrInvalid), err)
		}
		s.index.endings[target.Sequence] = ending{sequence: entry.Sequence, by: a.ID}
	case strings.HasPrefix(entry.Key, retractionPrefix):
		var r retractionRecord
		if err := json.Unmarshal(entry.Data, &r); err != nil {
			return err
		}
		target, ok, err := s.journal.Get(assertionPrefix + string(r.Assertion))
		if err != nil || !ok {
			return errors.Join(fmt.Errorf("%w: retracted assertion missing from journal", ErrInvalid), err)
		}
		s.index.endings[target.Sequence] = ending{sequence: entry.Sequence, retracted: true}
	default:
		return fmt.Errorf("%w: unknown journal record %q", ErrInvalid, entry.Key)
	}
	return nil
}

func keyPosting(kind KindID, key string) string { return journal.IndexKey(string(kind), key) }

func refKey(ref provenance.Ref) string {
	ref.Version = 0
	encoded, _ := json.Marshal(ref)
	return string(encoded)
}

func (s *Service) indexAssertion(a Assertion, sequence uint64) {
	s.index.all = append(s.index.all, sequence)
	s.index.byKind.Add(string(a.Kind), sequence)
	s.index.bySubject.Add(refKey(a.Subject), sequence)
	if a.Key != "" {
		s.index.byKey.Add(keyPosting(a.Kind, a.Key), sequence)
	}
	cited := map[string]bool{}
	for _, ref := range a.Evidence {
		key := refKey(ref)
		if !cited[key] {
			cited[key] = true
			s.index.byCited.Add(key, sequence)
		}
	}
	if a.Category == observability.Internal {
		s.index.internal[sequence] = true
	}
}

// Close stops operations and releases the root lease. If lease release fails, a
// later Close retries it instead of reporting success.
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

func closedError() error { return errors.Join(ErrClosed, journal.ErrClosed) }

func (s *Service) requireOpen() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return closedError()
	}
	return nil
}

func start(ctx context.Context, operation, key string) (context.Context, *observability.Span) {
	return observability.Start(ctx, observability.Scope{Layer: "assertions", Operation: operation}, key)
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
// wake, read Changes from your last position. Hooks end when the service closes.
func (s *Service) OnCommit(hook func(sequence uint64)) (stop func()) {
	return s.journal.OnCommit(hook)
}

// Change classes reported by Changes.
const (
	ChangeKind       = "kind"
	ChangeAssertion  = "assertion"
	ChangeRetraction = "retraction"
)

// Changes reads committed records in sequence order from a stored position. Class
// is ChangeKind, ChangeAssertion or ChangeRetraction, and Kind is the kind involved
// (empty for a retraction, which names its assertion through ID).
func (s *Service) Changes(ctx context.Context, q provenance.ChangeQuery) (page provenance.ChangePage, result error) {
	ctx, span := start(ctx, "changes", "")
	defer func() { span.End(result) }()
	if err := s.requireOpen(); err != nil {
		return page, err
	}
	return s.journal.Changes(ctx, q, func(entry journal.Entry) (provenance.Change, error) {
		switch {
		case strings.HasPrefix(entry.Key, kindPrefix):
			return provenance.Change{Class: ChangeKind, ID: strings.TrimPrefix(entry.Key, kindPrefix), Kind: strings.TrimPrefix(entry.Key, kindPrefix)}, nil
		case strings.HasPrefix(entry.Key, assertionPrefix):
			var a Assertion
			if err := json.Unmarshal(entry.Data, &a); err != nil {
				return provenance.Change{}, err
			}
			return provenance.Change{Class: ChangeAssertion, ID: string(a.ID), Kind: string(a.Kind), Activity: a.Activity, Actor: a.Actor, Internal: a.Category == observability.Internal}, nil
		}
		var r retractionRecord
		if err := json.Unmarshal(entry.Data, &r); err != nil {
			return provenance.Change{}, err
		}
		return provenance.Change{Class: ChangeRetraction, ID: string(r.Assertion), Activity: r.Activity, Actor: r.Actor, Internal: r.Category == observability.Internal}, nil
	})
}

// authenticate resolves a credential to its identity. Writers from another Authority
// fail here rather than being treated as absent.
func (s *Service) authenticate(writer Writer) (string, error) {
	if writer == (Writer{}) {
		return "", nil
	}
	if writer.authority == nil || writer.authority != s.options.Authority {
		return "", fmt.Errorf("%w: credential from another authority", ErrWriterForbidden)
	}
	return writer.identity, nil
}

func authorize(kind Kind, identity string) error {
	if len(kind.Writers) == 0 || identity != "" && slices.Contains(kind.Writers, identity) {
		return nil
	}
	return fmt.Errorf("%w: %q may not write kind %s", ErrWriterForbidden, identity, kind.ID())
}

func validKindName(name string) bool {
	if len(name) == 0 || len(name) > 128 || !provenance.ValidRecordID(name) {
		return false
	}
	return !strings.ContainsAny(name, "@/")
}

func (s *Service) checkNamespace(name, identity string) error {
	for prefix, allowed := range s.options.ReservedNamespaces {
		if strings.HasPrefix(name, prefix) && !(identity != "" && slices.Contains(allowed, identity)) {
			return fmt.Errorf("%w: %q may not register %s", ErrNamespace, identity, name)
		}
	}
	return nil
}

// Register commits a kind. Repeating an identical registration is a no-op; the same
// name and version with anything different is ErrConflict.
func (s *Service) Register(ctx context.Context, kind Kind, writer Writer) (id KindID, result error) {
	ctx, span := start(ctx, "register", kind.Name)
	defer func() { span.End(result) }()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := s.requireOpen(); err != nil {
		return "", err
	}
	if !validKindName(kind.Name) || kind.Version < 1 {
		return "", fmt.Errorf("%w: kind name or version", ErrInvalid)
	}
	if err := objects.ValidateShape(kind.Shape); err != nil {
		return "", errors.Join(ErrInvalid, err)
	}
	identity, err := s.authenticate(writer)
	if err != nil {
		return "", err
	}
	if err := s.checkNamespace(kind.Name, identity); err != nil {
		return "", err
	}
	if len(kind.Writers) > 0 && s.options.Authority == nil {
		return "", fmt.Errorf("%w: kind names writers but the service has no authority", ErrInvalid)
	}
	for _, w := range kind.Writers {
		if w == "" || len(w) > maxWriterBytes {
			return "", fmt.Errorf("%w: writer identity", ErrInvalid)
		}
	}
	kind.Writers = slices.Clone(kind.Writers)
	sort.Strings(kind.Writers)
	kind.Writers = slices.Compact(kind.Writers)
	data, err := canon.Marshal(kind)
	if err != nil {
		return "", errors.Join(ErrInvalid, err)
	}
	if len(data) > maxRecordBytes {
		return "", ErrLimit
	}
	id = kind.ID()
	_, _, err = s.journal.Commit(ctx, journal.Txn{Key: kindPrefix + string(id), Data: data, Apply: func(journal.Entry) {
		s.mu.Lock()
		s.index.kinds[id] = kind
		s.mu.Unlock()
	}})
	return id, translate(err)
}

// translate maps journal errors onto this package's sentinels, keeping the cause.
func translate(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, journal.ErrConflict):
		return errors.Join(ErrConflict, err)
	case errors.Is(err, journal.ErrCapacity):
		return errors.Join(ErrLimit, err)
	case errors.Is(err, journal.ErrClosed):
		return errors.Join(ErrClosed, err)
	}
	return err
}

// GetKind returns a registered kind.
func (s *Service) GetKind(ctx context.Context, id KindID) (Kind, error) {
	if err := ctx.Err(); err != nil {
		return Kind{}, err
	}
	if err := s.requireOpen(); err != nil {
		return Kind{}, err
	}
	s.mu.RLock()
	kind, ok := s.index.kinds[id]
	s.mu.RUnlock()
	if !ok {
		return Kind{}, fmt.Errorf("%w: kind %s", ErrNotFound, id)
	}
	kind.Writers = slices.Clone(kind.Writers)
	return kind, nil
}

func mintID() (ID, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}
	return ID("as-" + hex.EncodeToString(random[:])), nil
}

func (s *Service) validateRefs(ctx context.Context, refs ...provenance.Ref) error {
	for _, ref := range refs {
		if err := ref.Validate(); err != nil {
			return errors.Join(ErrInvalid, err)
		}
		if err := s.resolver.Check(ctx, ref); err != nil {
			if errors.Is(err, provenance.ErrUnsupportedForm) {
				return errors.Join(ErrInvalid, err)
			}
			return err
		}
	}
	return nil
}

// target is the part of an existing assertion a commit needs. It is read before the
// commit because records are immutable and a commit's Check may not read the journal.
type target struct {
	sequence uint64
	kind     KindID
	key      string
}

func (s *Service) loadTarget(id ID) (target, error) {
	entry, ok, err := s.journal.Get(assertionPrefix + string(id))
	if err != nil {
		return target{}, err
	}
	if !ok {
		return target{}, fmt.Errorf("%w: assertion %s", ErrNotFound, id)
	}
	var a Assertion
	if err := json.Unmarshal(entry.Data, &a); err != nil {
		return target{}, err
	}
	return target{sequence: entry.Sequence, kind: a.Kind, key: a.Key}, nil
}

// wrapRecord classifies a failed write, leaving errors that already carry their
// stage alone.
func wrapRecord(id ID, committed *bool, result *error) {
	if *result == nil {
		return
	}
	var stage *RecordError
	if !errors.As(*result, &stage) {
		*result = &RecordError{ID: id, Committed: *committed, Err: *result}
	}
}

// Record commits an assertion and everything that derives from it at once: the
// sequence stamp, the index entries, and the end of whatever it replaces. Repeating
// an identical request with the same ID returns that ID without changing anything,
// even if the assertion has since been replaced.
func (s *Service) Record(ctx context.Context, request Request) (id ID, result error) {
	category := observability.CategoryFrom(ctx)
	ctx, span := start(ctx, "record", string(request.ID))
	committed := false
	defer func() {
		wrapRecord(request.ID, &committed, &result)
		span.End(result)
	}()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := s.requireOpen(); err != nil {
		return "", err
	}
	identity, err := s.authenticate(request.Writer)
	if err != nil {
		return "", err
	}
	s.mu.RLock()
	kind, ok := s.index.kinds[request.Kind]
	s.mu.RUnlock()
	if !ok {
		return "", fmt.Errorf("%w: kind %s", ErrNotFound, request.Kind)
	}
	if err := authorize(kind, identity); err != nil {
		return "", err
	}
	if request.ID == "" {
		if request.ID, err = mintID(); err != nil {
			return "", err
		}
	}
	if !provenance.ValidRecordID(string(request.ID)) {
		return "", fmt.Errorf("%w: assertion ID", ErrInvalid)
	}
	if kind.Keyed != (request.Key != "") || len(request.Key) > maxKeyBytes {
		return "", fmt.Errorf("%w: keyed kinds require a key and others reject one", ErrInvalid)
	}
	if len(request.Evidence) > maxEvidence {
		return "", ErrLimit
	}
	value, err := canon.Normalize(request.Value)
	if err != nil {
		return "", errors.Join(ErrInvalid, err)
	}
	if err := objects.CheckJSON(value, kind.Shape); err != nil {
		return "", errors.Join(ErrInvalid, err)
	}
	internal := observability.WithInternal(ctx)
	if err := s.validateRefs(internal, append([]provenance.Ref{request.Subject}, request.Evidence...)...); err != nil {
		return "", err
	}
	if err := s.validateActivityField(internal, request.Activity); err != nil {
		return "", err
	}
	var replaced target
	if request.Replaces != "" {
		if replaced, err = s.loadTarget(request.Replaces); err != nil {
			return "", err
		}
		if replaced.kind != request.Kind || replaced.key != request.Key {
			return "", fmt.Errorf("%w: a replacement keeps its target's kind and key", ErrInvalid)
		}
	}
	data, err := canon.Marshal(Assertion{ID: request.ID, Kind: request.Kind, Subject: request.Subject, Value: value, Evidence: request.Evidence, Key: request.Key, Replaces: request.Replaces, Activity: request.Activity, Actor: request.Actor, Writer: identity, Category: category})
	if err != nil {
		return "", errors.Join(ErrInvalid, err)
	}
	if len(data) > maxRecordBytes {
		return "", ErrLimit
	}
	var stored Assertion
	if err := json.Unmarshal(data, &stored); err != nil {
		return "", err
	}
	txn := journal.Txn{Key: assertionPrefix + string(request.ID), Data: data}
	txn.Check = func() error { return s.checkRecord(request, replaced) }
	txn.Apply = func(entry journal.Entry) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.indexAssertion(stored, entry.Sequence)
		if request.Replaces != "" {
			s.index.endings[replaced.sequence] = ending{sequence: entry.Sequence, by: request.ID}
		}
	}
	_, reused, err := s.journal.Commit(ctx, txn)
	if err != nil {
		var commit *journal.CommitError
		committed = errors.As(err, &commit) && commit.Committed
		return request.ID, &RecordError{ID: request.ID, Committed: committed, Err: translate(err)}
	}
	committed = true
	if reused {
		span.Count("reused", 1)
	} else {
		span.Count("recorded", 1)
	}
	return request.ID, nil
}

func (s *Service) validateActivityField(ctx context.Context, activity string) error {
	if activity == "" {
		return nil
	}
	if s.options.ValidateActivity == nil {
		return fmt.Errorf("%w: no activity validator configured", ErrInvalid)
	}
	return s.options.ValidateActivity(ctx, activity)
}

// checkRecord runs inside the commit critical section, so its answer cannot go stale
// before the record lands. It is what makes current state race-free.
func (s *Service) checkRecord(request Request, replaced target) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if request.Replaces != "" {
		if end, ended := s.index.endings[replaced.sequence]; ended {
			return fmt.Errorf("%w: %s already ended at sequence %d", ErrConflict, request.Replaces, end.sequence)
		}
	}
	if request.Key == "" {
		return nil
	}
	list := s.index.byKey[keyPosting(request.Kind, request.Key)]
	if len(list) == 0 {
		return nil
	}
	last := list[len(list)-1]
	if _, ended := s.index.endings[last]; !ended && last != replaced.sequence {
		return fmt.Errorf("%w: kind %s key %q already has a current assertion", ErrConflict, request.Kind, request.Key)
	}
	return nil
}

// Retract ends an assertion without replacing it. Only a current assertion can be
// retracted, and only by a writer its kind accepts.
func (s *Service) Retract(ctx context.Context, r Retraction) (result error) {
	category := observability.CategoryFrom(ctx)
	ctx, span := start(ctx, "retract", string(r.Assertion))
	committed := false
	defer func() {
		wrapRecord(r.Assertion, &committed, &result)
		span.End(result)
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.requireOpen(); err != nil {
		return err
	}
	identity, err := s.authenticate(r.Writer)
	if err != nil {
		return err
	}
	assertion, err := s.loadTarget(r.Assertion)
	if err != nil {
		return err
	}
	s.mu.RLock()
	kind, ok := s.index.kinds[assertion.kind]
	s.mu.RUnlock()
	if !ok {
		return fmt.Errorf("%w: kind %s", ErrNotFound, assertion.kind)
	}
	if err := authorize(kind, identity); err != nil {
		return err
	}
	if err := s.validateActivityField(observability.WithInternal(ctx), r.Activity); err != nil {
		return err
	}
	data, err := canon.Marshal(retractionRecord{Assertion: r.Assertion, Activity: r.Activity, Actor: r.Actor, Writer: identity, Category: category})
	if err != nil {
		return errors.Join(ErrInvalid, err)
	}
	txn := journal.Txn{Key: retractionPrefix + string(r.Assertion), Data: data}
	txn.Check = func() error {
		s.mu.RLock()
		defer s.mu.RUnlock()
		if end, ended := s.index.endings[assertion.sequence]; ended {
			return fmt.Errorf("%w: %s already ended at sequence %d", ErrConflict, r.Assertion, end.sequence)
		}
		return nil
	}
	txn.Apply = func(entry journal.Entry) {
		s.mu.Lock()
		s.index.endings[assertion.sequence] = ending{sequence: entry.Sequence, retracted: true}
		s.mu.Unlock()
	}
	_, _, err = s.journal.Commit(ctx, txn)
	if err != nil {
		var commit *journal.CommitError
		committed = errors.As(err, &commit) && commit.Committed
		return &RecordError{ID: r.Assertion, Committed: committed, Err: translate(err)}
	}
	committed = true
	return nil
}

// ValidateAssertion reports whether an assertion exists. Wire it into another layer's
// options so references to assertions are checked there too.
func (s *Service) ValidateAssertion(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := s.loadTarget(ID(id))
	return err
}
