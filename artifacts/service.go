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
	"github.com/craggy-io/artifact-services/internal/crashpoint"
	"github.com/craggy-io/artifact-services/internal/journal"
	"github.com/craggy-io/artifact-services/internal/pager"
	"github.com/craggy-io/artifact-services/internal/typedclaim"
	"github.com/craggy-io/artifact-services/observability"
	"github.com/craggy-io/artifact-services/provenance"
	"github.com/craggy-io/artifact-services/storage"
)

const (
	defaultMaxEntries      = int64(100_000_000)
	defaultMaxMaterialized = 100_000
	defaultMaxManifest     = int64(canon.MaxBytes)
	defaultMaxLinkDepth    = 16
	blockCacheSize         = 8
)

// Options binds one exclusive catalog root and its persisted indexed-key policy.
// Zero limits select defaults. MaxRecords and MaxBytes bound the journal.
// MaxEntries bounds one manifest. MaxManifestBytes bounds the stored root object
// and cannot exceed 8 MiB. MaxMaterializedEntries bounds Get, which holds a whole
// entry list; ListFiles and OpenFile are unaffected by it. Relaxed skips the
// per-commit journal fsync: commits then survive process death but not OS crash.
type Options struct {
	Root                   string
	IndexedMetadata        []string
	MaxRecords             int
	MaxBytes               int64
	MaxEntries             int64
	MaxManifestBytes       int64
	MaxMaterializedEntries int
	MaxLinkDepth           int
	Relaxed                bool
	TempDir                string
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

// catalogIndex is the resident derived index. Records stay on disk in the journal.
type catalogIndex struct {
	schema      journal.Postings
	content     journal.Postings
	metadata    journal.Postings
	occArtifact journal.Postings
	occContent  journal.Postings
	occName     journal.Postings
	occMetadata journal.Postings
}

func newCatalogIndex() *catalogIndex {
	return &catalogIndex{journal.Postings{}, journal.Postings{}, journal.Postings{}, journal.Postings{}, journal.Postings{}, journal.Postings{}, journal.Postings{}}
}

// Service owns the authoritative registration journal and a synchronous derived
// catalog index. Close releases the exclusive root lease; already-open readers
// remain caller-owned. Roots must not be shared with another live service/process.
type Service struct {
	blobs     cas.Store
	records   *journal.Store
	indexed   map[string]bool
	options   Options
	mu        sync.RWMutex
	index     *catalogIndex
	closed    bool
	closeDone bool
	files     *pager.Cache[Entry]
	searches  *pager.Cache[uint64]
	// fence lets reclaim tooling exclude every publication's check-then-register.
	fence sync.RWMutex
	cache blockCache
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
	if options.MaxEntries == 0 {
		options.MaxEntries = defaultMaxEntries
	}
	if options.MaxManifestBytes == 0 {
		options.MaxManifestBytes = defaultMaxManifest
	}
	if options.MaxMaterializedEntries == 0 {
		options.MaxMaterializedEntries = defaultMaxMaterialized
	}
	if options.MaxLinkDepth == 0 {
		options.MaxLinkDepth = defaultMaxLinkDepth
	}
	if options.MaxEntries < 0 || options.MaxManifestBytes < 0 || options.MaxManifestBytes > defaultMaxManifest || options.MaxMaterializedEntries < 0 || options.MaxLinkDepth < 0 {
		return nil, fmt.Errorf("%w: limit out of range", ErrLimit)
	}
	config, err := canon.Marshal(struct {
		Version int
		Indexed []string
	}{ManifestVersion, keys})
	if err != nil {
		return nil, err
	}
	records, err := journal.Open(options.Root, journal.Options{Config: config, MaxRecords: options.MaxRecords, MaxBytes: options.MaxBytes, Relaxed: options.Relaxed})
	if err != nil {
		return nil, err
	}
	result := &Service{blobs: blobs, records: records, indexed: indexed, options: options, index: newCatalogIndex(), files: pager.New[Entry](), searches: pager.New[uint64]()}
	if err := result.loadIndex(context.Background()); err != nil {
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
	if s.closeDone {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	// Not under s.mu: a commit in flight holds the journal lock and wants s.mu.
	if err := s.records.Close(); err != nil {
		return err
	}
	s.mu.Lock()
	s.closeDone = true
	s.mu.Unlock()
	return nil
}

// Sequence returns the catalog journal's latest commit sequence. It increases by
// exactly one per new record and survives restart.
func (s *Service) Sequence(ctx context.Context) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := s.requireOpen(); err != nil {
		return 0, err
	}
	return s.records.Sequence(), nil
}

// OnCommit registers a hook fired after each durable catalog commit with the newest
// sequence. Calls are serial, may skip sequences, and never block a commit: on any
// wake, read Changes from your last position. The stop function may be called by
// the hook itself. Hooks end when the service closes.
func (s *Service) OnCommit(hook func(sequence uint64)) (stop func()) {
	return s.records.OnCommit(hook)
}

// Change classes for artifact catalog feeds.
const (
	ChangeArtifact   = "artifact"
	ChangeOccurrence = "occurrence"
)

func decodeChange(entry journal.Entry) (provenance.Change, error) {
	if id, ok := strings.CutPrefix(entry.Key, occurrenceKeyPrefix); ok {
		var o Occurrence
		if err := json.Unmarshal(entry.Data, &o); err != nil {
			return provenance.Change{}, err
		}
		return provenance.Change{Class: ChangeOccurrence, ID: id, Kind: o.Name, Activity: o.Producer, Internal: o.Category == observability.Internal}, nil
	}
	var m membership
	if err := json.Unmarshal(entry.Data, &m); err != nil {
		return provenance.Change{}, err
	}
	return provenance.Change{Class: ChangeArtifact, ID: entry.Key, Kind: string(m.SchemaID), Internal: m.Category == observability.Internal}, nil
}

// Changes reads committed catalog records in sequence order from a stored position.
func (s *Service) Changes(ctx context.Context, q provenance.ChangeQuery) (page provenance.ChangePage, result error) {
	ctx, span := start(ctx, "changes", "")
	defer func() { span.End(result) }()
	if err := s.requireOpen(); err != nil {
		return page, err
	}
	return s.records.Changes(ctx, q, decodeChange)
}

// Fence runs fn with every publication's check-then-register excluded, so content
// that fn finds unreferenced cannot become referenced until it returns.
func (s *Service) Fence(ctx context.Context, fn func(context.Context) error) error {
	s.fence.Lock()
	defer s.fence.Unlock()
	return fn(ctx)
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

func (s *Service) publish(ctx context.Context, input Manifest, claim *acceptedClaim) (ID, error) {
	if input.Version != 0 && input.Version != ManifestVersion {
		return "", &PublishError{Err: invalidf("unsupported version")}
	}
	if int64(len(input.Entries)) > s.options.MaxEntries {
		return "", &PublishError{Err: invalidf("entry limit")}
	}
	entries := append([]Entry{}, input.Entries...)
	sort.SliceStable(entries, func(i, j int) bool { return comparePaths(entries[i].Path, entries[j].Path) < 0 })
	return s.commit(ctx, publication{
		metadata: input.Metadata,
		claim:    claim,
		each: func(visit func(Entry) error) error {
			for _, e := range entries {
				if err := validateEntry(e); err != nil {
					return err
				}
				if err := visit(e); err != nil {
					return err
				}
			}
			return nil
		},
		verifyContent: s.verifyDistinct(),
	})
}

// verifyDistinct re-hashes each distinct stream through EOF, even when content is
// already registered; repeated entries for one blob reuse its verified size.
func (s *Service) verifyDistinct() func(context.Context, Entry) error {
	verified := map[cas.ID]int64{}
	return func(ctx context.Context, entry Entry) error {
		size, seen := verified[entry.Content]
		if !seen {
			var err error
			if size, err = s.hashThrough(ctx, entry.Content); err != nil {
				return err
			}
			verified[entry.Content] = size
		}
		if size != entry.Size {
			return invalidf("size mismatch for %q", entry.Path)
		}
		return nil
	}
}

func (s *Service) hashThrough(ctx context.Context, id cas.ID) (int64, error) {
	reader, err := s.blobs.Open(ctx, id)
	if err != nil {
		return 0, err
	}
	size, readErr := io.Copy(io.Discard, reader)
	return size, errors.Join(readErr, reader.Close())
}

// publication is one manifest moving through the pipeline. each yields validated
// entries in comparePaths order. verifyContent checks one regular file.
type publication struct {
	metadata      map[string]json.RawMessage
	claim         *acceptedClaim
	each          func(func(Entry) error) error
	verifyContent func(context.Context, Entry) error
}

// commit is the only path to the catalog: blocks, then the root, then the journal.
func (s *Service) commit(ctx context.Context, p publication) (id ID, resultErr error) {
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
	metadata, err := normalizeMetadata(p.metadata, false)
	if err != nil {
		return "", err
	}
	support := observability.WithInternal(ctx)
	var source storedManifest
	if p.claim != nil {
		if _, err := ParseID(p.claim.Schema); err != nil {
			return "", err
		}
		if _, err := ParseID(p.claim.Data); err != nil {
			return "", err
		}
		if source, err = s.loadRoot(support, ID(p.claim.Data)); err != nil {
			return "", err
		}
		if _, err := s.loadRoot(support, ID(p.claim.Schema)); err != nil {
			return "", err
		}
		if metadata == nil {
			metadata = map[string]json.RawMessage{}
		}
		for key, value := range p.claim.values() {
			raw, _ := json.Marshal(value)
			metadata[key] = raw
		}
	}
	// Publication holds the fence from its first content check until its record
	// commits, so a reclaim sweep can never delete what it has just checked.
	s.fence.RLock()
	defer s.fence.RUnlock()

	var cleanup []error
	stageErr := func(err error) (bool, error) {
		if err == nil {
			return true, nil
		}
		var outcome *cas.PutError
		if errors.As(err, &outcome) && outcome.Committed {
			cleanup = append(cleanup, err)
			return true, nil
		}
		return false, err
	}
	writer := &blockWriter{}
	writer.flush = func(encoded []byte, first string, count int) error {
		put, err := s.blobs.Put(support, bytes.NewReader(encoded))
		if ok, err := stageErr(err); !ok {
			return err
		}
		writer.root.Blocks = append(writer.root.Blocks, blockRef{ID: put.ID, First: first, Count: count})
		return nil
	}
	depth := 0
	if err := p.each(func(e Entry) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		switch e.Kind {
		case "":
			if err := p.verifyContent(support, e); err != nil {
				return err
			}
		case KindLink:
			d, err := s.checkLink(support, e)
			if err != nil {
				return err
			}
			depth = max(depth, d)
		}
		if writer.root.Entries >= s.options.MaxEntries {
			return invalidf("entry limit")
		}
		return writer.add(e)
	}); err != nil {
		return "", err
	}
	if err := writer.finish(); err != nil {
		return "", err
	}
	root := writer.root
	root.LinkDepth = depth
	root.Metadata = metadata
	if p.claim != nil {
		a, _ := canon.Marshal(source.Blocks)
		b, _ := canon.Marshal(root.Blocks)
		if !bytes.Equal(a, b) {
			return "", fmt.Errorf("%w: claim source entries differ", ErrUnvalidated)
		}
	}
	encoded, err := canon.Marshal(root)
	if err != nil {
		return "", err
	}
	if int64(len(encoded)) > s.options.MaxManifestBytes {
		return "", invalidf("manifest byte limit")
	}
	if _, err := decodeRoot(encoded, s.options.MaxManifestBytes); err != nil {
		return "", err
	}
	record, err := s.describe(root, category, p.claim)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoded)
	id = ID("sha256:" + hex.EncodeToString(digest[:]))
	record.ID = id
	crashpoint.Hit(crashpoint.ArtifactsVerified)
	put, err := s.blobs.Put(support, bytes.NewReader(encoded))
	available, err = stageErr(err)
	if !available {
		return id, err
	}
	if put.ID != cas.ID(id) {
		return id, errors.Join(append(cleanup, fmt.Errorf("%w: CAS returned unexpected manifest ID", ErrInvalidManifest))...)
	}
	crashpoint.Hit(crashpoint.ArtifactsStored)
	serialized, err := canon.Marshal(record)
	if err != nil {
		return id, errors.Join(append(cleanup, err)...)
	}
	if err := s.requireOpen(); err != nil {
		return id, errors.Join(append(cleanup, err)...)
	}
	_, reused, err = s.records.Commit(ctx, journal.Txn{Key: string(id), Data: serialized, Apply: func(e journal.Entry) {
		s.mu.Lock()
		s.index.addMembership(e.Sequence, record)
		s.mu.Unlock()
	}})
	if errors.Is(err, journal.ErrClosed) {
		err = errors.Join(closedError(), err)
	}
	var commit *journal.CommitError
	registered = err == nil || (errors.As(err, &commit) && commit.Committed)
	if errors.Is(err, journal.ErrConflict) {
		err = errors.Join(ErrConflict, err)
	}
	if errors.Is(err, journal.ErrCapacity) {
		err = errors.Join(ErrLimit, err)
	}
	if registered {
		crashpoint.Hit(crashpoint.ArtifactsRegistered)
	}
	return id, errors.Join(append(cleanup, err)...)
}

// checkLink requires the target registered, and any sub-path to exist in it. It
// returns the link's resulting depth, bounded so link chains cannot grow unbounded.
func (s *Service) checkLink(ctx context.Context, e Entry) (int, error) {
	if _, err := s.registered(ctx, e.Target); err != nil {
		return 0, invalidf("link target %s: %w", e.Target, err)
	}
	target, err := s.loadRoot(ctx, e.Target)
	if err != nil {
		return 0, err
	}
	if e.TargetPath != "" {
		if _, err := s.resolvePath(ctx, e.Target, e.TargetPath, 0); err != nil {
			if !errors.Is(err, ErrNotFound) {
				return 0, err
			}
			// A target path may also be an implicit directory with entries below it.
			found := false
			if err := s.walk(ctx, target, e.TargetPath, "", false, 0, func(entry Entry) (bool, error) {
				found = strings.HasPrefix(entry.Path, e.TargetPath+"/")
				return true, nil
			}); err != nil {
				return 0, err
			}
			if !found {
				return 0, invalidf("link target path %q not in %s", e.TargetPath, e.Target)
			}
		}
	}
	depth := target.LinkDepth + 1
	if depth > s.options.MaxLinkDepth {
		return 0, invalidf("link depth %d exceeds %d", depth, s.options.MaxLinkDepth)
	}
	return depth, nil
}

func (c acceptedClaim) values() map[string]string {
	return map[string]string{"objects.schema": c.Schema, "objects.data": c.Data, "objects.validator": c.Validator, "objects.encoding": c.Encoding}
}

func (s *Service) describe(root storedManifest, category observability.Category, claim *acceptedClaim) (membership, error) {
	content, err := root.contentID()
	if err != nil {
		return membership{}, err
	}
	record := membership{CatalogItem: CatalogItem{FileCount: root.Files, LogicalBytes: root.Bytes, Content: content, Category: category}, Metadata: map[string]json.RawMessage{}, Claim: claim}
	for key := range s.indexed {
		if value, ok := root.Metadata[key]; ok {
			normalized, err := canon.IndexedScalar(value)
			if err != nil {
				return membership{}, invalidf("metadata %s: %w", key, err)
			}
			record.Metadata[key] = normalized
		}
	}
	if claim != nil {
		record.SchemaID = ID(claim.Schema)
	}
	return record, nil
}

func (s *Service) loadRoot(ctx context.Context, id ID) (storedManifest, error) {
	if _, err := ParseID(string(id)); err != nil {
		return storedManifest{}, err
	}
	reader, err := s.blobs.Open(observability.WithInternal(ctx), cas.ID(id))
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return storedManifest{}, errors.Join(ErrNotFound, err)
		}
		return storedManifest{}, err
	}
	raw, err := readBounded(reader, s.options.MaxManifestBytes)
	if err != nil {
		return storedManifest{}, err
	}
	return decodeRoot(raw, s.options.MaxManifestBytes)
}

type cachedBlock struct {
	id      cas.ID
	entries []Entry
}

// blockCache keeps a few decoded blocks so paging and repeated single-file opens
// on one large tree do not re-hash the same block. Blocks are immutable, verified
// through EOF before entering, and never handed out without copying.
type blockCache struct {
	mu     sync.Mutex
	blocks []cachedBlock
}

func (c *blockCache) get(id cas.ID) []Entry {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, block := range c.blocks {
		if block.id == id {
			return block.entries
		}
	}
	return nil
}
func (c *blockCache) put(id cas.ID, entries []Entry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.blocks) >= blockCacheSize {
		c.blocks = c.blocks[1:]
	}
	c.blocks = append(c.blocks, cachedBlock{id, entries})
}

func (s *Service) loadBlock(ctx context.Context, ref blockRef) ([]Entry, error) {
	if cached := s.cache.get(ref.ID); cached != nil {
		return cached, nil
	}
	reader, err := s.blobs.Open(observability.WithInternal(ctx), ref.ID)
	if err != nil {
		return nil, err
	}
	raw, err := readBounded(reader, maxBlockBytes+maxBlockEntries+4096)
	if err != nil {
		return nil, err
	}
	entries, err := decodeBlock(raw, ref)
	if err != nil {
		return nil, err
	}
	s.cache.put(ref.ID, entries)
	return entries, nil
}

// Get materializes a whole manifest, up to MaxMaterializedEntries: larger ones
// return ErrLimit and are read with ListFiles and OpenFile. It resolves known
// immutable manifests even if catalog registration failed. Reserved metadata is
// not itself proof of accepted typed publication; only ValidatedClaim consults
// that authoritative acceptance.
func (s *Service) Get(ctx context.Context, id ID) (manifest Manifest, resultErr error) {
	ctx, span := start(ctx, "get", string(id))
	defer func() { span.End(resultErr) }()
	if err := ctx.Err(); err != nil {
		return Manifest{}, err
	}
	if err := s.requireOpen(); err != nil {
		return Manifest{}, err
	}
	root, err := s.loadRoot(ctx, id)
	if err != nil {
		return Manifest{}, err
	}
	if root.Entries > int64(s.options.MaxMaterializedEntries) {
		return Manifest{}, fmt.Errorf("%w: manifest has %d entries; use ListFiles", ErrLimit, root.Entries)
	}
	manifest = Manifest{Version: root.Version, Entries: make([]Entry, 0, root.Entries), Metadata: root.Metadata}
	for _, ref := range root.Blocks {
		entries, err := s.loadBlock(ctx, ref)
		if err != nil {
			return Manifest{}, err
		}
		manifest.Entries = append(manifest.Entries, entries...)
	}
	span.Count("returned", 1)
	return manifest, nil
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
	root, err := s.loadRoot(ctx, id)
	if err != nil {
		return typedclaim.Claim{}, err
	}
	for key, value := range member.Claim.values() {
		raw, _ := json.Marshal(value)
		if !bytes.Equal(root.Metadata[key], raw) {
			return typedclaim.Claim{}, ErrUnvalidated
		}
	}
	c := member.Claim
	return typedclaim.New(c.Schema, c.Data, c.Validator, c.Encoding), nil
}

func decodeMembership(entry journal.Entry) (membership, error) {
	var saved membership
	if err := json.Unmarshal(entry.Data, &saved); err != nil {
		return membership{}, err
	}
	if string(saved.ID) != entry.Key || saved.Category > observability.Internal {
		return membership{}, errors.New("artifacts: corrupt membership")
	}
	saved.Sequence = entry.Sequence
	return saved, nil
}

func (x *catalogIndex) addMembership(sequence uint64, record membership) {
	if record.SchemaID != "" {
		x.schema.Add(string(record.SchemaID), sequence)
	}
	x.content.Add(string(record.Content), sequence)
	for key, value := range record.Metadata {
		x.metadata.Add(journal.IndexKey(key, string(value)), sequence)
	}
}

func (x *catalogIndex) addOccurrence(sequence uint64, o Occurrence) {
	x.occArtifact.Add(string(o.Artifact), sequence)
	x.occContent.Add(string(o.Content), sequence)
	if o.Name != "" {
		x.occName.Add(o.Name, sequence)
	}
	for key, value := range o.indexed {
		x.occMetadata.Add(journal.IndexKey(key, string(value)), sequence)
	}
}

// loadIndex rebuilds only the resident index, by streaming the journal once. It
// reads no CAS content, so opening does not scale with stored payload.
func (s *Service) loadIndex(ctx context.Context) error {
	index := newCatalogIndex()
	return errors.Join(s.records.Scan(ctx, 0, 0, func(entry journal.Entry) error {
		if strings.HasPrefix(entry.Key, occurrenceKeyPrefix) {
			o, err := s.decodeOccurrence(entry)
			if err != nil {
				return err
			}
			index.addOccurrence(entry.Sequence, o)
			return nil
		}
		record, err := decodeMembership(entry)
		if err != nil {
			return err
		}
		index.addMembership(entry.Sequence, record)
		return nil
	}), func() error {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.index = index
		return nil
	}())
}

// Rebuild deep-validates authoritative registrations against their immutable
// manifests and atomically replaces the derived index. It reads every registered
// root from CAS, so it is an explicit verification step, not part of opening.
// Existing cursor snapshots survive.
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
		watermark := s.records.Sequence()
		rebuilt := newCatalogIndex()
		records := int64(0)
		err := s.records.Scan(ctx, 0, watermark, func(entry journal.Entry) error {
			if strings.HasPrefix(entry.Key, occurrenceKeyPrefix) {
				o, err := s.decodeOccurrence(entry)
				if err != nil {
					return err
				}
				rebuilt.addOccurrence(entry.Sequence, o)
				return nil
			}
			record, err := s.verifyMembership(ctx, entry)
			if err != nil {
				return err
			}
			rebuilt.addMembership(entry.Sequence, record)
			records++
			return nil
		})
		if err != nil {
			return err
		}
		swapped := false
		s.records.View(func(sequence uint64) {
			s.mu.Lock()
			defer s.mu.Unlock()
			if !s.closed && sequence == watermark {
				s.index = rebuilt
				swapped = true
			}
		})
		if swapped {
			span.Count("records", records)
			return nil
		}
	}
}

func (s *Service) verifyMembership(ctx context.Context, entry journal.Entry) (membership, error) {
	saved, err := decodeMembership(entry)
	if err != nil {
		return membership{}, err
	}
	root, err := s.loadRoot(ctx, saved.ID)
	if err != nil {
		return membership{}, err
	}
	expected, err := s.describe(root, saved.Category, saved.Claim)
	if err != nil {
		return membership{}, err
	}
	expected.ID = saved.ID
	if saved.Claim != nil {
		for key, value := range saved.Claim.values() {
			raw, _ := json.Marshal(value)
			if !bytes.Equal(root.Metadata[key], raw) {
				return membership{}, ErrUnvalidated
			}
		}
	} else {
		for key := range root.Metadata {
			if strings.HasPrefix(key, "objects.") {
				return membership{}, ErrUnvalidated
			}
		}
	}
	saved.Sequence = 0
	a, _ := canon.Marshal(expected)
	b, _ := canon.Marshal(saved)
	if !bytes.Equal(a, b) {
		return membership{}, errors.New("artifacts: membership summary mismatch")
	}
	expected.Sequence = entry.Sequence
	return expected, nil
}

// registered reads immutable authority directly from the journal.
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
	entry, ok, err := s.records.Get(string(id))
	if err != nil {
		return membership{}, err
	}
	if !ok {
		return membership{}, ErrNotFound
	}
	member, err := decodeMembership(entry)
	if err != nil || member.ID != id {
		return membership{}, errors.Join(errors.New("artifacts: corrupt registration"), err)
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

// Reachable visits every content ID that registered artifacts depend on: each
// root, each entry block and each regular file. Repeats are not filtered. Callers
// that delete on this basis must run inside Fence.
func (s *Service) Reachable(ctx context.Context, visit func(cas.ID) error) (result error) {
	ctx, span := start(ctx, "reachable", "")
	defer func() { span.End(result) }()
	if err := s.requireOpen(); err != nil {
		return err
	}
	return s.records.Scan(ctx, 0, 0, func(entry journal.Entry) error {
		if strings.HasPrefix(entry.Key, occurrenceKeyPrefix) {
			return nil
		}
		id := ID(entry.Key)
		root, err := s.loadRoot(ctx, id)
		if err != nil {
			return fmt.Errorf("artifact %s: %w", id, err)
		}
		if err := visit(cas.ID(id)); err != nil {
			return err
		}
		for _, ref := range root.Blocks {
			if err := visit(ref.ID); err != nil {
				return err
			}
			entries, err := s.loadBlock(ctx, ref)
			if err != nil {
				return fmt.Errorf("artifact %s block %s: %w", id, ref.ID, err)
			}
			for _, e := range entries {
				if e.IsFile() {
					if err := visit(e.Content); err != nil {
						return err
					}
				}
			}
		}
		return nil
	})
}
