// Package storage provides immutable, keyed blob storage without assigning or
// verifying content identity. Keys are 2–128 lowercase hexadecimal characters.
package storage

import (
	"errors"
	"io"
)

var (
	ErrInvalidKey    = errors.New("storage: invalid key")
	ErrNotFound      = errors.New("storage: blob not found")
	ErrAlreadyExists = errors.New("storage: blob already exists")
)

// Store stores opaque bytes under caller-supplied keys. Implementations must be
// safe for concurrent use, including independent instances sharing a backend.
type Store interface {
	// Put publishes a complete blob without replacing an existing key. Concurrent
	// writes to one key have one winner; other writers return ErrAlreadyExists.
	// The reader is not closed and may be consumed even if the key exists.
	// Source errors must be returned without publishing partial content.
	Put(key string, source io.Reader) error
	// Open returns a reader owned by the caller, or ErrNotFound. Subsequent read
	// and close errors are returned by the reader itself.
	Open(key string) (io.ReadCloser, error)
	// Exists returns false, nil for a missing key, and propagates other errors.
	Exists(key string) (bool, error)
}
