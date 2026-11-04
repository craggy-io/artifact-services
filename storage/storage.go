// Package storage provides immutable, keyed blob storage without assigning or
// verifying content identity. Keys are 2–128 lowercase hexadecimal characters.
package storage

import (
	"context"
	"errors"
	"io"
	"time"
)

var (
	ErrInvalidKey    = errors.New("storage: invalid key")
	ErrNotFound      = errors.New("storage: blob not found")
	ErrAlreadyExists = errors.New("storage: blob already exists")
)

// PutError records whether this Put call published a complete blob before failing.
// Err must be non-nil and remains accessible through Unwrap. Store implementations
// must wrap Put failures in this type, even when the source error is a PutError
// from another operation. Use errors.As to inspect the outermost PutError.
type PutError struct {
	Published bool
	Err       error
}

func (e *PutError) Error() string {
	if e == nil || e.Err == nil {
		return "storage: put"
	}
	return "storage: put: " + e.Err.Error()
}
func (e *PutError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// Store stores opaque bytes under caller-supplied keys. Implementations must be
// safe for concurrent use, including independent instances sharing a backend.
// Pass a non-nil context; implementations propagate it and explicitly report
// their storage work through observability. Cancellation is cooperative and
// cannot interrupt arbitrary blocked readers or filesystem operations.
type Store interface {
	// Put publishes a complete blob without replacing an existing key. Concurrent
	// writes to one key have one winner; other writers return ErrAlreadyExists.
	// The reader is not closed and may be consumed even if the key exists.
	// Source errors must be returned without publishing partial content.
	// Nil means published. Failures must be wrapped in PutError, whose
	// Published field describes this call, not any nested source operation.
	Put(ctx context.Context, key string, source io.Reader) error
	// Open returns a reader owned by the caller, or ErrNotFound. Subsequent read
	// and close errors are returned by the reader itself.
	Open(ctx context.Context, key string) (io.ReadCloser, error)
	// Exists returns false, nil for a missing key, and propagates other errors.
	Exists(ctx context.Context, key string) (bool, error)
}

// Info describes a published blob.
type Info struct {
	Size    int64
	ModTime time.Time
}

// Stater is an optional capability: metadata without opening the blob.
type Stater interface {
	Stat(ctx context.Context, key string) (Info, error)
}

// File is a seekable blob handle, owned by the caller.
type File interface {
	io.ReadSeekCloser
	io.ReaderAt
}

// Seekable is an optional capability for backends whose blobs are plain files at
// a stable location. Storage verifies nothing: see cas.Service.OpenSeekable.
type Seekable interface {
	OpenSeekable(ctx context.Context, key string) (File, error)
}

// Reclaimer is an optional capability that deliberately breaks immutability's
// "published means forever", for tooling that has fenced writers itself.
type Reclaimer interface {
	Walk(ctx context.Context, visit func(key string, info Info) error) error
	Remove(ctx context.Context, key string, olderThan time.Time) (bool, error)
	SweepPending(ctx context.Context, olderThan time.Time) (int, error)
}
