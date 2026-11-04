// Package cas stores immutable bytes by SHA-256 content identity. It has no
// filenames, revision histories, or metadata search.
package cas

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/craggy-io/artifact-services/observability"
	"github.com/craggy-io/artifact-services/storage"
)

// ID is an algorithm-qualified digest: sha256 followed by 64 lowercase hex digits.
type ID string

var (
	ErrInvalidID = errors.New("cas: invalid content ID")
	ErrIntegrity = errors.New("cas: content digest mismatch")
)

// ParseID validates the supported algorithm and canonical digest spelling.
func ParseID(value string) (ID, error) {
	if len(value) != 71 || !strings.HasPrefix(value, "sha256:") {
		return "", ErrInvalidID
	}
	for _, c := range value[7:] {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return "", ErrInvalidID
		}
	}
	return ID(value), nil
}

// PutResult identifies the consumed content after hashing completes. A nonempty
// ID alone does not prove availability: check the error/PutError.Committed too.
type PutResult struct {
	ID     ID
	Size   int64
	Reused bool
}

// PutError records whether this operation established verified content availability
// (new publication or verified reuse) before a later failure. Err retains all causes.
// Every failed Put wraps its cause so an input PutError cannot masquerade as this
// call's committed outcome. Reused distinguishes existing content in the result.
type PutError struct {
	Committed bool
	Err       error
}

func (e *PutError) Error() string { return "cas: put: " + e.Err.Error() }
func (e *PutError) Unwrap() error { return e.Err }

// Store is a streaming content-addressable store. Returned readers are owned by
// callers and verified only on successful EOF, never merely on Open or early Close.
type Store interface {
	Put(context.Context, io.Reader) (PutResult, error)
	Open(context.Context, ID) (io.ReadCloser, error)
	Exists(context.Context, ID) (bool, error)
}

// Options configures staging. Empty TempDir uses the OS temporary directory.
// A configured directory must already exist; it is not created by New.
type Options struct{ TempDir string }

// Service implements Store over a storage backend. It is safe for concurrent
// operations. The backend/root must be exclusively managed as immutable storage.
type Service struct {
	blobs   storage.Store
	tempDir string
}

var _ Store = (*Service)(nil)

// New constructs a CAS without creating files. A relative TempDir is resolved
// now; its existence and permissions are checked when a write stages data.
func New(blobs storage.Store, options Options) (*Service, error) {
	if blobs == nil {
		return nil, errors.New("cas: nil storage")
	}
	dir := options.TempDir
	if dir != "" {
		var err error
		dir, err = filepath.Abs(dir)
		if err != nil {
			return nil, err
		}
	}
	return &Service{blobs: blobs, tempDir: dir}, nil
}

func start(ctx context.Context, op string, key string) (context.Context, *observability.Span) {
	return observability.Start(ctx, observability.Scope{Layer: "cas", Operation: op}, key)
}

func storageKey(id ID) (string, error) {
	if _, err := ParseID(string(id)); err != nil {
		return "", err
	}
	return string(id)[7:], nil
}

// Put hashes a stream into a private temporary file and then publishes it. Source
// readers are not closed. Verified reuse is a successful logical write. Cleanup
// failure can return a committed result and error; nothing is rolled back.
func (s *Service) Put(ctx context.Context, source io.Reader) (PutResult, error) {
	return s.put(ctx, source, os.Remove)
}

// removeTemp is a narrow test seam for the post-publication cleanup boundary.
func (s *Service) put(ctx context.Context, source io.Reader, removeTemp func(string) error) (result PutResult, resultErr error) {
	ctx, span := start(ctx, "put", "")
	committed := false
	defer func() {
		if resultErr != nil {
			resultErr = &PutError{Committed: committed, Err: resultErr}
		}
		if committed {
			if result.Reused {
				span.Count("reused", 1)
			} else {
				span.Count("published", 1)
			}
		}
		span.End(resultErr)
	}()
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if source == nil {
		return result, errors.New("cas: nil source")
	}
	spool, err := os.CreateTemp(s.tempDir, "cas-pending-*")
	if err != nil {
		return result, err
	}
	defer func() {
		resultErr = errors.Join(resultErr, spool.Close())
		if err := removeTemp(spool.Name()); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("cas: staging cleanup: %w", err))
		}
	}()
	digest := sha256.New()
	source = &countedReader{ctx: ctx, source: source, span: span}
	if _, err := io.Copy(io.MultiWriter(spool, digest), source); err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	size, err := spool.Seek(0, io.SeekCurrent)
	if err != nil {
		return result, err
	}
	result = PutResult{ID: ID("sha256:" + hex.EncodeToString(digest.Sum(nil))), Size: size}
	if _, err := spool.Seek(0, io.SeekStart); err != nil {
		return result, err
	}
	key, _ := storageKey(result.ID)
	err = s.blobs.Put(ctx, key, spool)
	if err == nil {
		committed = true
		return result, nil
	}
	var outcome *storage.PutError
	if errors.As(err, &outcome) && outcome.Published {
		committed = true
		return result, err
	}
	if !errors.Is(err, storage.ErrAlreadyExists) {
		return result, err
	}
	verified, verifyErr := s.verifyExisting(ctx, result.ID)
	if !verified {
		return result, errors.Join(verifyErr, nonDuplicateError(err))
	}
	result.Reused = true
	committed = true
	return result, errors.Join(verifyErr, nonDuplicateError(err))
}

// A normal already-exists result includes the filesystem's existence error. Any
// additional failure (such as cleanup) must survive even after successful reuse.
func nonDuplicateError(err error) error {
	if onlyDuplicate(err) {
		return nil
	}
	return err
}
func onlyDuplicate(err error) bool {
	if err == nil {
		return true
	}
	if group, ok := err.(interface{ Unwrap() []error }); ok {
		for _, cause := range group.Unwrap() {
			if !onlyDuplicate(cause) {
				return false
			}
		}
		return true
	}
	if wrapper, ok := err.(interface{ Unwrap() error }); ok {
		return onlyDuplicate(wrapper.Unwrap())
	}
	return errors.Is(err, storage.ErrAlreadyExists) || errors.Is(err, fs.ErrExist)
}

func (s *Service) verifyExisting(ctx context.Context, id ID) (bool, error) {
	reader, err := s.Open(ctx, id)
	if err != nil {
		return false, err
	}
	_, readErr := io.Copy(io.Discard, reader)
	closeErr := reader.Close()
	return readErr == nil, errors.Join(readErr, closeErr)
}

// Open streams data while hashing it. A mismatch replaces successful EOF with
// ErrIntegrity. Bytes can be delivered before verification. Cancellation is
// cooperative; it cannot interrupt an arbitrary blocked backend reader.
func (s *Service) Open(ctx context.Context, id ID) (reader io.ReadCloser, resultErr error) {
	ctx, span := start(ctx, "open", string(id))
	defer func() {
		if resultErr != nil {
			span.End(resultErr)
		}
	}()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	key, err := storageKey(id)
	if err != nil {
		return nil, err
	}
	base, err := s.blobs.Open(ctx, key)
	if err != nil {
		return nil, err
	}
	return &verifiedReader{ctx: ctx, base: base, span: span, digest: sha256.New(), expected: string(id)[7:]}, nil
}

// Exists checks presence, not integrity. Missing content is false, nil; other
// backend errors retain their identity through the storage implementation.
func (s *Service) Exists(ctx context.Context, id ID) (exists bool, resultErr error) {
	ctx, span := start(ctx, "exists", string(id))
	defer func() { span.End(resultErr) }()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	key, err := storageKey(id)
	if err != nil {
		return false, err
	}
	return s.blobs.Exists(ctx, key)
}
