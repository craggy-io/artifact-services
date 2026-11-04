package artifacts

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	"github.com/craggy-io/artifact-services/cas"
	"github.com/craggy-io/artifact-services/internal/canon"
	"github.com/craggy-io/artifact-services/internal/journal"
	"github.com/craggy-io/artifact-services/internal/pager"
	"github.com/craggy-io/artifact-services/internal/typedclaim"
	"github.com/craggy-io/artifact-services/observability"
	"github.com/craggy-io/artifact-services/storage"
)

// Options binds one exclusive catalog root and its persisted indexed-key policy.
type Options struct {
	Root            string
	IndexedMetadata []string
}

type acceptedClaim struct {
	Schema    string `json:"schema"`
	Data      string `json:"data"`
	Validator string `json:"validator"`
	Encoding  string `json:"encoding"`
}
type membership struct {
	CatalogItem
	Metadata map[string]json.RawMessage `json:"metadata,omitempty"`
	Claim    *acceptedClaim             `json:"claim,omitempty"`
}

// Service owns the authoritative registration journal and a synchronous derived
// catalog. Close releases the exclusive root lease; already-open readers remain
// caller-owned. Roots must not be shared with another live service/process.
type Service struct {
	blobs     cas.Store
	records   *journal.Store
	indexed   map[string]bool
	mu        sync.RWMutex
	catalog   map[ID]membership
	closed    bool
	closeDone bool
	files     *pager.Cache[Entry]
	searches  *pager.Cache[CatalogItem]
}

var _ Store = (*Service)(nil)

func New(blobs cas.Store, options Options) (*Service, error) {
	if blobs == nil {
		return nil, errors.New("artifacts: nil CAS")
	}
	keys := append([]string(nil), options.IndexedMetadata...)
	sort.Strings(keys)
	if len(keys) > 16 {
		return nil, fmt.Errorf("%w: indexed-key limit", ErrInvalidQuery)
	}
	indexed := map[string]bool{}
	for _, key := range keys {
		if key == "" || len(key) > 256 || strings.HasPrefix(key, "objects.") || indexed[key] {
			return nil, fmt.Errorf("%w: indexed key", ErrInvalidQuery)
		}
		indexed[key] = true
	}
	config, err := canon.Marshal(struct {
		Version int
		Indexed []string
	}{1, keys})
	if err != nil {
		return nil, err
	}
	records, err := journal.Open(options.Root, journal.Options{Config: config})
	if err != nil {
		return nil, err
	}
	result := &Service{blobs: blobs, records: records, indexed: indexed, files: pager.New[Entry](), searches: pager.New[CatalogItem]()}
	if err := result.Rebuild(context.Background()); err != nil {
		return nil, errors.Join(err, records.Close())
	}
	return result, nil
}

func start(ctx context.Context, op string, key string) (context.Context, *observability.Span) {
	return observability.Start(ctx, observability.Scope{Layer: "artifacts", Operation: op}, key)
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

// Close stops operations and releases the root lease. If lease release fails,
// a later Close retries it instead of reporting success.
func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closeDone {
		return nil
	}
	s.closed = true
	if err := s.records.Close(); err != nil {
		return err
	}
	s.closeDone = true
	return nil
}

func (s *Service) Publish(ctx context.Context, manifest Manifest) (ID, error) {
	return s.publish(ctx, manifest, nil)
}

// PublishValidated is module-internal capability-gated publication. External
// consumers cannot construct the private typedclaim.Claim argument. Objects must
// validate source bytes before issuing that capability; arbitrary metadata cannot.
func (s *Service) PublishValidated(ctx context.Context, manifest Manifest, claim typedclaim.Claim) (ID, error) {
	schema, data, validator, encoding, valid := claim.Fields()
	if !valid {
		return "", &PublishError{Err: ErrUnvalidated}
	}
	return s.publish(ctx, manifest, &acceptedClaim{Schema: schema, Data: data, Validator: validator, Encoding: encoding})
}

func (s *Service) publish(ctx context.Context, input Manifest, claim *acceptedClaim) (id ID, resultErr error) {
	ctx, span := start(ctx, "publish", "")
	category := observability.CategoryFrom(ctx)
	available, registered, reused := false, false, false
	defer func() {
		if registered {
			if reused {
				span.Count("reused", 1)
			} else {
				span.Count("published", 1)
			}
		}
		if resultErr != nil {
			resultErr = &PublishError{ID: id, ManifestCommitted: available, Registered: registered, Err: resultErr}
		}
		span.End(resultErr)
	}()
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := s.requireOpen(); err != nil {
		return "", err
	}
	manifest, _, err := normalizeManifest(input, false)
	if err != nil {
		return "", err
	}
	support := observability.WithInternal(ctx)
	if claim != nil {
		if _, err := ParseID(claim.Schema); err != nil {
			return "", err
		}
		if _, err := ParseID(claim.Data); err != nil {
			return "", err
		}
		source, err := s.Get(support, ID(claim.Data))
		if err != nil {
			return "", err
		}
		if _, err := s.Get(support, ID(claim.Schema)); err != nil {
			return "", err
		}
		a, _ := canon.Marshal(source.Entries)
		b, _ := canon.Marshal(manifest.Entries)
		if !bytes.Equal(a, b) {
			return "", fmt.Errorf("%w: claim source entries differ", ErrUnvalidated)
		}
		if manifest.Metadata == nil {
			manifest.Metadata = map[string]json.RawMessage{}
		}
		for key, value := range claim.values() {
			raw, _ := json.Marshal(value)
			manifest.Metadata[key] = raw
		}
	}
	manifest, encoded, err := normalizeManifest(manifest, true)
	if err != nil {
		return "", err
	}
	record, err := s.describe(manifest, category, claim)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	id = ID("sha256:" + hex.EncodeToString(digest[:]))
	record.ID = id
	// Verify every distinct referenced stream through EOF, even when content is
	// already registered; repeated entries for one blob reuse its verified size.
	verified := map[cas.ID]int64{}
	for _, entry := range manifest.Entries {
		if err := ctx.Err(); err != nil {
			return id, err
		}
		size, seen := verified[entry.Content]
		if !seen {
			reader, err := s.blobs.Open(support, entry.Content)
			if err != nil {
				return id, err
			}
			var readErr error
			size, readErr = io.Copy(io.Discard, reader)
			err = errors.Join(readErr, reader.Close())
			if err != nil {
				return id, err
			}
			verified[entry.Content] = size
		}
		if size != entry.Size {
			return id, fmt.Errorf("%w: size mismatch for %q", ErrInvalidManifest, entry.Path)
		}
	}
	put, putErr := s.blobs.Put(support, bytes.NewReader(encoded))
	var outcome *cas.PutError
	available = putErr == nil || (errors.As(putErr, &outcome) && outcome.Committed)
	if !available {
		return id, putErr
	}
	if put.ID != cas.ID(id) {
		return id, errors.Join(putErr, fmt.Errorf("%w: CAS returned unexpected manifest ID", ErrInvalidManifest))
	}
	serialized, err := canon.Marshal(record)
	if err != nil {
		return id, errors.Join(putErr, err)
	}
	// Append (including its file sync) runs outside the service lock so one slow
	// registration does not stall concurrent reads; the journal's own mutex and
	// idempotent/conflict semantics serialize writers. The lock covers only the
	// in-memory projection.
	if err := s.requireOpen(); err != nil {
		return id, errors.Join(putErr, err)
	}
	_, reused, err = s.records.Append(ctx, string(id), serialized)
	if errors.Is(err, journal.ErrClosed) {
		err = errors.Join(closedError(), err)
	}
	var commit *journal.CommitError
	registered = err == nil || (errors.As(err, &commit) && commit.Committed)
	if registered {
		s.mu.Lock()
		if !s.closed {
			s.catalog[id] = record
		}
		s.mu.Unlock()
	}
	if errors.Is(err, journal.ErrConflict) {
		err = errors.Join(ErrConflict, err)
	}
	return id, errors.Join(putErr, err)
}

func (c acceptedClaim) values() map[string]string {
	return map[string]string{"objects.schema": c.Schema, "objects.data": c.Data, "objects.validator": c.Validator, "objects.encoding": c.Encoding}
}

func (s *Service) describe(manifest Manifest, category observability.Category, claim *acceptedClaim) (membership, error) {
	record := membership{CatalogItem: CatalogItem{FileCount: int64(len(manifest.Entries)), Category: category}, Metadata: map[string]json.RawMessage{}, Claim: claim}
	for _, entry := range manifest.Entries {
		record.LogicalBytes += entry.Size
	}
	for key := range s.indexed {
		if value, ok := manifest.Metadata[key]; ok {
			normalized, err := indexedScalar(value)
			if err != nil {
				return membership{}, fmt.Errorf("%w: metadata %s: %w", ErrInvalidManifest, key, err)
			}
			record.Metadata[key] = normalized
		}
	}
	if claim != nil {
		record.SchemaID = ID(claim.Schema)
	}
	return record, nil
}

func (s *Service) getRaw(ctx context.Context, id ID) (Manifest, error) {
	if _, err := ParseID(string(id)); err != nil {
		return Manifest{}, err
	}
	reader, err := s.blobs.Open(observability.WithInternal(ctx), cas.ID(id))
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return Manifest{}, errors.Join(ErrNotFound, err)
		}
		return Manifest{}, err
	}
	raw, err := readBounded(reader, MaxManifestBytes)
	if err != nil {
		return Manifest{}, err
	}
	return decodeManifest(raw)
}

// Get resolves known immutable manifests even if catalog registration failed.
// Reserved metadata is not itself proof of accepted typed publication; only
// ValidatedClaim consults that authoritative acceptance.
func (s *Service) Get(ctx context.Context, id ID) (manifest Manifest, resultErr error) {
	ctx, span := start(ctx, "get", string(id))
	defer func() { span.End(resultErr) }()
	if err := ctx.Err(); err != nil {
		return Manifest{}, err
	}
	if err := s.requireOpen(); err != nil {
		return Manifest{}, err
	}
	manifest, resultErr = s.getRaw(ctx, id)
	if resultErr == nil {
		span.Count("returned", 1)
	}
	return
}

// ValidatedClaim returns module-private trusted claim data only from authoritative
// registered acceptance, then checks its immutable manifest binding.
func (s *Service) ValidatedClaim(ctx context.Context, id ID) (typedclaim.Claim, error) {
	member, err := s.registered(ctx, id)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return typedclaim.Claim{}, errors.Join(ErrUnvalidated, err)
		}
		return typedclaim.Claim{}, err
	}
	if member.Claim == nil {
		return typedclaim.Claim{}, ErrUnvalidated
	}
	manifest, err := s.Get(observability.WithInternal(ctx), id)
	if err != nil {
		return typedclaim.Claim{}, err
	}
	for key, value := range member.Claim.values() {
		raw, _ := json.Marshal(value)
		if !bytes.Equal(manifest.Metadata[key], raw) {
			return typedclaim.Claim{}, ErrUnvalidated
		}
	}
	c := member.Claim
	return typedclaim.New(c.Schema, c.Data, c.Validator, c.Encoding), nil
}

// Rebuild validates authoritative registrations against immutable manifests and
// atomically replaces the derived catalog. Existing cursor snapshots survive.
func (s *Service) Rebuild(ctx context.Context) (resultErr error) {
	ctx, span := start(ctx, "rebuild", "")
	defer func() { span.End(resultErr) }()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.requireOpen(); err != nil {
			return err
		}
		entries, watermark := s.records.Snapshot()
		rebuilt := make(map[ID]membership, len(entries))
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return err
			}
			var saved membership
			if err := json.Unmarshal(entry.Data, &saved); err != nil {
				return err
			}
			if string(saved.ID) != entry.Key || saved.Category > observability.Internal {
				return fmt.Errorf("artifacts: corrupt membership")
			}
			manifest, err := s.getRaw(ctx, saved.ID)
			if err != nil {
				return err
			}
			expected, err := s.describe(manifest, saved.Category, saved.Claim)
			if err != nil {
				return err
			}
			expected.ID = saved.ID
			if saved.Claim != nil {
				for key, value := range saved.Claim.values() {
					raw, _ := json.Marshal(value)
					if !bytes.Equal(manifest.Metadata[key], raw) {
						return ErrUnvalidated
					}
				}
			} else {
				for key := range manifest.Metadata {
					if strings.HasPrefix(key, "objects.") {
						return ErrUnvalidated
					}
				}
			}
			a, _ := canon.Marshal(expected)
			b, _ := canon.Marshal(saved)
			if !bytes.Equal(a, b) {
				return errors.New("artifacts: membership summary mismatch")
			}
			rebuilt[saved.ID] = expected
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return closedError()
		}
		_, current := s.records.Snapshot()
		if current == watermark {
			s.catalog = rebuilt
			s.mu.Unlock()
			span.Count("records", int64(len(rebuilt)))
			return nil
		}
		s.mu.Unlock()
	}
}

// registered reads immutable authority rather than searching the derived catalog.
func (s *Service) registered(ctx context.Context, id ID) (membership, error) {
	if err := ctx.Err(); err != nil {
		return membership{}, err
	}
	if err := s.requireOpen(); err != nil {
		return membership{}, err
	}
	if _, err := ParseID(string(id)); err != nil {
		return membership{}, err
	}
	entry, ok := s.records.Get(string(id))
	if !ok {
		return membership{}, ErrNotFound
	}
	var member membership
	if err := json.Unmarshal(entry.Data, &member); err != nil {
		return membership{}, err
	}
	if member.ID != id || member.Category > observability.Internal {
		return membership{}, errors.New("artifacts: corrupt registration")
	}
	return member, nil
}

// Category resolves the authoritative classification of a registered artifact.
// Unregistered known-ID manifests remain readable with Get but return ErrNotFound.
func (s *Service) Category(ctx context.Context, id ID) (category observability.Category, resultErr error) {
	ctx, span := start(ctx, "category", string(id))
	defer func() { span.End(resultErr) }()
	member, err := s.registered(ctx, id)
	if err != nil {
		return observability.User, err
	}
	return member.Category, nil
}
