// Package spool gives any verified stream random access by copying it to a temporary
// file and handing that back only after the digest checked out at EOF. It costs a full
// copy per open. Use it for backends without seekable support, or when bytes must be
// proven before processing starts.
package spool

import (
	"context"
	"errors"
	"io"
	"os"

	"github.com/craggy-io/artifact-services/artifacts"
	"github.com/craggy-io/artifact-services/cas"
)

// File is a verified private copy. Close removes it.
type File struct {
	*os.File
	path string
}

// Close closes and removes the copy.
func (f *File) Close() error {
	return errors.Join(f.File.Close(), os.Remove(f.path))
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}

// Copy drains source into a temporary file under dir, where empty means the OS
// default, and returns it rewound. A source that is an integrity-checking reader fails
// at EOF on corruption, so success means the bytes were verified. Copy closes source.
func Copy(ctx context.Context, source io.ReadCloser, dir string) (file *File, result error) {
	defer func() { result = errors.Join(result, source.Close()) }()
	temp, err := os.CreateTemp(dir, "spool-*")
	if err != nil {
		return nil, err
	}
	path := temp.Name()
	defer func() {
		if result != nil {
			result = errors.Join(result, temp.Close(), os.Remove(path))
		}
	}()
	if _, err := io.Copy(temp, contextReader{ctx, source}); err != nil {
		return nil, err
	}
	if _, err := temp.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	return &File{File: temp, path: path}, nil
}

// OpenFile returns random access to an artifact file, proven before return. It uses
// the backend's seekable path when there is one, which verifies on open without a
// copy, and falls back to spooling.
func OpenFile(ctx context.Context, store *artifacts.Service, id artifacts.ID, path, dir string) (cas.SeekableFile, error) {
	file, err := store.OpenSeekable(ctx, id, path)
	if err == nil {
		return file, nil
	}
	if !errors.Is(err, cas.ErrCapability) {
		return nil, err
	}
	reader, err := store.OpenFile(ctx, id, path)
	if err != nil {
		return nil, err
	}
	return Copy(ctx, reader, dir)
}
