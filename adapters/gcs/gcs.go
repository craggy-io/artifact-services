// Package gcs is a shared blob backend: a storage.Store over one Google Cloud
// Storage bucket, safe for any number of processes at once.
//
// Put creates an object only if none exists (ifGenerationMatch=0), so concurrent
// writers of one key have one winner and the rest get storage.ErrAlreadyExists. An
// upload becomes visible only when it completes, so a failed source never publishes
// partial content. Like the filesystem store, a Put that finds the key existing
// refreshes the object's custom time, which guards it from a concurrent garbage
// collection sweep (see Remove).
//
// It implements Stater and Reclaimer, not Seekable: cas.OpenSeekable reports
// ErrCapability for it, so callers spool instead (adapters/spool).
//
// The caller owns the client. Tests and local development point it at an emulator
// with STORAGE_EMULATOR_HOST.
package gcs

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"

	"github.com/craggy-io/artifact-services/observability"
	blobs "github.com/craggy-io/artifact-services/storage"
)

// Store keeps each blob as one object named Prefix + key.
type Store struct {
	bucket *storage.BucketHandle
	prefix string
}

var (
	_ blobs.Store     = (*Store)(nil)
	_ blobs.Stater    = (*Store)(nil)
	_ blobs.Reclaimer = (*Store)(nil)
)

// New returns a store over bucket. prefix may be empty; otherwise it should end
// with a slash.
func New(client *storage.Client, bucket, prefix string) (*Store, error) {
	if client == nil || bucket == "" {
		return nil, errors.New("gcs: client and bucket are required")
	}
	return &Store{bucket: client.Bucket(bucket), prefix: prefix}, nil
}

func start(ctx context.Context, operation, key string) (context.Context, *observability.Span) {
	return observability.Start(ctx, observability.Scope{Layer: "storage", Operation: operation}, key)
}

func (s *Store) object(key string) (*storage.ObjectHandle, error) {
	if err := blobs.ValidateKey(key); err != nil {
		return nil, err
	}
	return s.bucket.Object(s.prefix + key), nil
}

func status(err error) int {
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) {
		return apiErr.Code
	}
	return 0
}

func notFound(err error) bool {
	return errors.Is(err, storage.ErrObjectNotExist) || status(err) == http.StatusNotFound
}

func preconditionFailed(err error) bool { return status(err) == http.StatusPreconditionFailed }

// Put uploads source as a new object.
func (s *Store) Put(ctx context.Context, key string, source io.Reader) (result error) {
	ctx, span := start(ctx, "put", key)
	published := false
	defer func() {
		if result != nil {
			result = &blobs.PutError{Published: published, Err: result}
		}
		if published {
			span.Count("published", 1)
		}
		span.End(result)
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	object, err := s.object(key)
	if err != nil {
		return err
	}
	if source == nil {
		return errors.New("storage: nil source")
	}
	// Cancelling the upload's context is the only way to abandon it unpublished.
	upload, cancel := context.WithCancel(ctx)
	defer cancel()
	w := object.If(storage.Conditions{DoesNotExist: true}).NewWriter(upload)
	w.ContentType = "application/octet-stream"
	w.CustomTime = time.Now().UTC()
	written, err := io.Copy(w, source)
	span.Count("bytes", written)
	if err != nil {
		cancel()
		_ = w.Close()
		return err
	}
	if err := ctx.Err(); err != nil {
		cancel()
		_ = w.Close()
		return err
	}
	if err := w.Close(); err != nil {
		if preconditionFailed(err) {
			return errors.Join(blobs.ErrAlreadyExists, err, s.touch(ctx, object))
		}
		return err
	}
	published = true
	return nil
}

// touch moves an existing object's custom time forward, so Remove spares it.
func (s *Store) touch(ctx context.Context, object *storage.ObjectHandle) error {
	_, err := object.Update(ctx, storage.ObjectAttrsToUpdate{CustomTime: time.Now().UTC()})
	if notFound(err) {
		return nil // reclaimed meanwhile: the caller's ErrAlreadyExists is now stale, and its retry publishes
	}
	return err
}

// Open streams a blob. Read errors arrive from the reader.
func (s *Store) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	ctx, span := start(ctx, "open", key)
	object, err := s.object(key)
	if err != nil {
		span.End(err)
		return nil, err
	}
	r, err := object.NewReader(ctx)
	if notFound(err) {
		err = errors.Join(blobs.ErrNotFound, err)
	}
	if err != nil {
		span.End(err)
		return nil, err
	}
	return &reader{r: r, span: span}, nil
}

type reader struct {
	r    *storage.Reader
	span *observability.Span
	read int64
}

func (r *reader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.read += int64(n)
	return n, err
}

func (r *reader) Close() error {
	err := r.r.Close()
	r.span.Count("bytes", r.read)
	r.span.End(err)
	return err
}

// Exists reports whether a blob exists.
func (s *Store) Exists(ctx context.Context, key string) (exists bool, result error) {
	_, err := s.Stat(ctx, key)
	if errors.Is(err, blobs.ErrNotFound) {
		return false, nil
	}
	return err == nil, err
}

// Stat reports a blob's size and the later of its creation and custom time.
func (s *Store) Stat(ctx context.Context, key string) (info blobs.Info, result error) {
	ctx, span := start(ctx, "stat", key)
	defer func() { span.End(result) }()
	object, err := s.object(key)
	if err != nil {
		return blobs.Info{}, err
	}
	attrs, err := object.Attrs(ctx)
	if notFound(err) {
		return blobs.Info{}, errors.Join(blobs.ErrNotFound, err)
	}
	if err != nil {
		return blobs.Info{}, err
	}
	return infoOf(attrs), nil
}

func infoOf(attrs *storage.ObjectAttrs) blobs.Info {
	modified := attrs.Created
	if attrs.CustomTime.After(modified) {
		modified = attrs.CustomTime
	}
	return blobs.Info{Size: attrs.Size, ModTime: modified}
}

// Walk visits every blob under the prefix in name order.
func (s *Store) Walk(ctx context.Context, visit func(key string, info blobs.Info) error) (result error) {
	ctx, span := start(ctx, "walk", "")
	defer func() { span.End(result) }()
	it := s.bucket.Objects(ctx, &storage.Query{Prefix: s.prefix})
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return nil
		}
		if err != nil {
			return err
		}
		key := strings.TrimPrefix(attrs.Name, s.prefix)
		if blobs.ValidateKey(key) != nil {
			continue
		}
		if err := visit(key, infoOf(attrs)); err != nil {
			return err
		}
	}
}

// Remove deletes a blob only if it was created and last re-Put before olderThan.
// The delete is conditional on the generation and metageneration it checked, so a
// Put that touches the blob in between makes it fail and the blob survives.
func (s *Store) Remove(ctx context.Context, key string, olderThan time.Time) (removed bool, result error) {
	ctx, span := start(ctx, "remove", key)
	defer func() {
		if removed {
			span.Count("removed", 1)
		}
		span.End(result)
	}()
	object, err := s.object(key)
	if err != nil {
		return false, err
	}
	attrs, err := object.Attrs(ctx)
	if notFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !infoOf(attrs).ModTime.Before(olderThan) {
		return false, nil
	}
	err = object.If(storage.Conditions{GenerationMatch: attrs.Generation, MetagenerationMatch: attrs.Metageneration}).Delete(ctx)
	if notFound(err) || preconditionFailed(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// SweepPending removes nothing: an unfinished upload is never an object.
func (s *Store) SweepPending(context.Context, time.Time) (int, error) { return 0, nil }
