package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/craggy-io/artifact-services/internal/crashpoint"
	"github.com/craggy-io/artifact-services/internal/durable"
)

const pendingPrefix = ".pending-"

// Filesystem stores one file per blob in a private, trusted directory tree.
// The filesystem must support atomic hard-link creation without replacement.
// The zero value is not usable; construct with NewFilesystem.
type Filesystem struct{ root string }

var (
	_ Store     = (*Filesystem)(nil)
	_ Stater    = (*Filesystem)(nil)
	_ Seekable  = (*Filesystem)(nil)
	_ Reclaimer = (*Filesystem)(nil)
)

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
	if s == nil || s.root == "" {
		return "", errors.New("storage: Filesystem not constructed with NewFilesystem")
	}
	if err := ValidateKey(key); err != nil {
		return "", err
	}
	return filepath.Join(s.root, key[:2], key), nil
}

// Put writes, syncs and closes a temporary file, publishes a hard link, then syncs
// that namespace change, so a nil return survives OS crash and power loss on a local
// filesystem. An already-existing key has its modification time refreshed, because
// that time is what makes it safe from a concurrent reclaim sweep (see Remove).
// Cleanup failures are joined with the operation error; a cleanup failure after
// publication returns an error even though the complete blob is already visible.
func (s *Filesystem) Put(ctx context.Context, key string, source io.Reader) (result error) {
	return s.put(ctx, key, source, os.Remove)
}

// removeTemp is injected only by package tests to exercise cleanup failures.
func (s *Filesystem) put(ctx context.Context, key string, source io.Reader, removeTemp func(string) error) (result error) {
	ctx, span := startOperation(ctx, "put", key)
	published := false
	defer func() {
		if result != nil {
			result = &PutError{Published: published, Err: result}
		}
		if published {
			span.Count("published", 1)
		}
		span.End(result)
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
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
	f, err := os.CreateTemp(dir, pendingPrefix+"*")
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			result = errors.Join(result, f.Close())
		}
		if err := removeTemp(f.Name()); err != nil {
			result = errors.Join(result, fmt.Errorf("storage: temporary cleanup: %w", err))
		}
	}()
	if _, err := io.Copy(contextWriter{ctx: ctx, dest: f}, &streamReader{ctx: ctx, base: source, span: span}); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
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
	if err := ctx.Err(); err != nil {
		return err
	}
	crashpoint.Hit(crashpoint.StorageTempSynced)
	if err := os.Link(f.Name(), path); err != nil {
		if errors.Is(err, fs.ErrExist) {
			now := time.Now()
			return errors.Join(ErrAlreadyExists, err, os.Chtimes(path, now, now))
		}
		return err
	}
	published = true
	crashpoint.Hit(crashpoint.StorageLinked)
	return durable.Published(path, s.root)
}

// Open opens a published blob. It never exposes a pending write.
func (s *Filesystem) Open(ctx context.Context, key string) (reader io.ReadCloser, result error) {
	ctx, span := startOperation(ctx, "open", key)
	defer func() {
		if result != nil {
			span.End(result)
		}
	}()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
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
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, f.Close())
	}
	return &blobReader{streamReader: streamReader{ctx: ctx, base: f, span: span}, closer: f}, nil
}

// Exists reports whether a published regular blob exists.
func (s *Filesystem) Exists(ctx context.Context, key string) (exists bool, result error) {
	ctx, span := startOperation(ctx, "exists", key)
	defer func() { span.End(result) }()
	if err := ctx.Err(); err != nil {
		return false, err
	}
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

// Stat reports a published blob's size and modification time without opening it.
func (s *Filesystem) Stat(ctx context.Context, key string) (info Info, result error) {
	ctx, span := startOperation(ctx, "stat", key)
	defer func() { span.End(result) }()
	if err := ctx.Err(); err != nil {
		return Info{}, err
	}
	path, err := s.blobPath(key)
	if err != nil {
		return Info{}, err
	}
	found, err := os.Stat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return Info{}, errors.Join(ErrNotFound, err)
	}
	if err := requireRegular(found, err); err != nil {
		return Info{}, err
	}
	return Info{Size: found.Size(), ModTime: found.ModTime()}, nil
}

// OpenSeekable returns the backing file itself, so no bytes are copied. The handle
// is owned by the caller.
func (s *Filesystem) OpenSeekable(ctx context.Context, key string) (file File, result error) {
	ctx, span := startOperation(ctx, "open_seekable", key)
	defer func() { span.End(result) }()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
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

// eachShard visits the entries of every two-character shard directory.
func (s *Filesystem) eachShard(ctx context.Context, visit func(dir string, file fs.DirEntry) error) error {
	if s == nil || s.root == "" {
		return errors.New("storage: Filesystem not constructed with NewFilesystem")
	}
	shards, err := os.ReadDir(s.root)
	if err != nil {
		return err
	}
	for _, shard := range shards {
		if !shard.IsDir() {
			continue
		}
		dir := filepath.Join(s.root, shard.Name())
		files, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		for _, file := range files {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := visit(dir, file); err != nil {
				return err
			}
		}
	}
	return nil
}

// Walk visits published blobs in no particular order. Temporary files left by a
// crash are not visited; see SweepPending.
func (s *Filesystem) Walk(ctx context.Context, visit func(key string, info Info) error) (result error) {
	ctx, span := startOperation(ctx, "walk", "")
	defer func() { span.End(result) }()
	return s.eachShard(ctx, func(dir string, blob fs.DirEntry) error {
		name := blob.Name()
		if path, err := s.blobPath(name); err != nil || filepath.Dir(path) != dir {
			return nil
		}
		found, err := blob.Info()
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if !found.Mode().IsRegular() {
			return nil
		}
		return visit(name, Info{Size: found.Size(), ModTime: found.ModTime()})
	})
}

// Remove deletes a blob only if it was last written or re-Put before olderThan.
// Put refreshes the time of a key it finds existing, so content that a writer is
// currently reusing fails this check and survives. Removing is not atomic with any
// caller's reachability analysis: the caller fences that itself.
func (s *Filesystem) Remove(ctx context.Context, key string, olderThan time.Time) (removed bool, result error) {
	ctx, span := startOperation(ctx, "remove", key)
	defer func() {
		if removed {
			span.Count("removed", 1)
		}
		span.End(result)
	}()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	path, err := s.blobPath(key)
	if err != nil {
		return false, err
	}
	found, err := os.Stat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err := requireRegular(found, err); err != nil {
		return false, err
	}
	if !found.ModTime().Before(olderThan) {
		return false, nil
	}
	if err := os.Remove(path); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	return true, durable.SyncDir(filepath.Dir(path))
}

// SweepPending removes temporary files older than olderThan: crash leftovers that
// were never published and that no reader could ever have seen.
func (s *Filesystem) SweepPending(ctx context.Context, olderThan time.Time) (removed int, result error) {
	ctx, span := startOperation(ctx, "sweep_pending", "")
	defer func() {
		span.Count("removed", int64(removed))
		span.End(result)
	}()
	result = s.eachShard(ctx, func(dir string, file fs.DirEntry) error {
		if !strings.HasPrefix(file.Name(), pendingPrefix) {
			return nil
		}
		found, err := file.Info()
		if err != nil || !found.ModTime().Before(olderThan) {
			return nil
		}
		if err := os.Remove(filepath.Join(dir, file.Name())); err != nil {
			return err
		}
		removed++
		return nil
	})
	return removed, result
}
