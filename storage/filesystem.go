package storage

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

// Filesystem stores one file per blob in a private, trusted directory tree.
// The filesystem must support atomic hard-link creation without replacement.
// The zero value is not usable; construct with NewFilesystem.
type Filesystem struct{ root string }

var _ Store = (*Filesystem)(nil)

// NewFilesystem creates root if necessary. Relative roots are resolved now.
// Callers must prevent external modifications and symlinks within this tree.
func NewFilesystem(root string) (*Filesystem, error) {
	if root == "" {
		return nil, errors.New("storage: empty root")
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(abs, 0700); err != nil {
		return nil, err
	}
	return &Filesystem{root: abs}, nil
}

func (s *Filesystem) blobPath(key string) (string, error) {
	if len(key) < 2 || len(key) > 128 {
		return "", ErrInvalidKey
	}
	for _, c := range key {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return "", ErrInvalidKey
		}
	}
	return filepath.Join(s.root, key[:2], key), nil
}

// Put writes, syncs and closes a temporary file before publishing a hard link.
// Cleanup failures are joined with the operation error; a cleanup failure after
// publication returns an error even though the complete blob is already visible.
func (s *Filesystem) Put(key string, source io.Reader) (result error) {
	path, err := s.blobPath(key)
	if err != nil {
		return err
	}
	if source == nil {
		return errors.New("storage: nil source")
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".pending-*")
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			result = errors.Join(result, f.Close())
		}
		if err := os.Remove(f.Name()); err != nil {
			result = errors.Join(result, fmt.Errorf("storage: temporary cleanup: %w", err))
		}
	}()
	if _, err := io.Copy(f, source); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	err = f.Close()
	closed = true
	if err != nil {
		return err
	}
	if err := os.Link(f.Name(), path); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return errors.Join(ErrAlreadyExists, err)
		}
		return err
	}
	return nil
}

// Open opens a published blob. It never exposes a pending write.
func (s *Filesystem) Open(key string) (io.ReadCloser, error) {
	path, err := s.blobPath(key)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, errors.Join(ErrNotFound, err)
	}
	if err != nil {
		return nil, err
	}
	if err := requireRegular(f.Stat()); err != nil {
		return nil, errors.Join(err, f.Close())
	}
	return f, nil
}

// Exists reports whether a published regular blob exists.
func (s *Filesystem) Exists(key string) (bool, error) {
	path, err := s.blobPath(key)
	if err != nil {
		return false, err
	}
	info, err := os.Stat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err := requireRegular(info, err); err != nil {
		return false, err
	}
	return true, nil
}

func requireRegular(info fs.FileInfo, err error) error {
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("storage: blob is not a regular file")
	}
	return nil
}
