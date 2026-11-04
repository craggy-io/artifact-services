package artifacts

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/craggy-io/artifact-services/cas"
	"github.com/craggy-io/artifact-services/internal/canon"
	"github.com/craggy-io/artifact-services/internal/extsort"
)

// BuilderOptions configures one streaming publication. MaxMemory bounds the sort
// buffer; beyond it entries spill to TempDir. VerifyContent re-hashes every regular
// file through EOF; otherwise presence and size are checked against the backend,
// which relies on CAS having hashed the content when it was Put. A backend with no
// metadata capability is always re-hashed.
type BuilderOptions struct {
	Metadata      map[string]json.RawMessage
	VerifyContent bool
	MaxMemory     int
	TempDir       string
}

// Builder assembles a manifest of any size without holding its entries in memory.
// Entries may arrive in any order. It is not safe for concurrent use. Close must
// always be called; it is a no-op after Publish has already released the spill.
type Builder struct {
	service *Service
	options BuilderOptions
	sorter  *extsort.Sorter
	closed  bool
}

// NewBuilder starts a publication. Nothing is visible until Publish commits.
func (s *Service) NewBuilder(options BuilderOptions) *Builder {
	if options.TempDir == "" {
		options.TempDir = s.options.TempDir
	}
	return &Builder{service: s, options: options, sorter: extsort.New(extsort.Options{Compare: compareRecords, MaxMemory: options.MaxMemory, TempDir: options.TempDir})}
}

func compareRecords(a, b []byte) int {
	x, _, _ := bytes.Cut(a, []byte{0})
	y, _, _ := bytes.Cut(b, []byte{0})
	return comparePaths(string(x), string(y))
}

// Add validates one entry and queues it. Duplicates, and entries nested under a
// non-directory, are reported by Publish, once all entries are in order.
func (b *Builder) Add(e Entry) error {
	if b.closed {
		return errors.New("artifacts: builder closed")
	}
	if err := validateEntry(e); err != nil {
		return err
	}
	if b.sorter.Count() >= b.service.options.MaxEntries {
		return invalidf("entry limit")
	}
	encoded, err := canon.Marshal(e)
	if err != nil {
		return err
	}
	record := make([]byte, 0, len(e.Path)+1+len(encoded))
	record = append(record, e.Path...)
	record = append(record, 0)
	record = append(record, encoded...)
	return b.sorter.Add(record)
}

// Count returns the number of entries added.
func (b *Builder) Count() int64 { return b.sorter.Count() }

// verifier selects how Publish checks each regular file. Adjacent duplicates are
// checked once, which covers the common case of a blob shared by sibling paths.
func (b *Builder) verifier() func(context.Context, Entry) error {
	statter, stats := b.service.blobs.(cas.Statter)
	var last cas.ID
	var lastSize int64
	return func(ctx context.Context, e Entry) error {
		if e.Content != last {
			var err error
			if stats && !b.options.VerifyContent {
				lastSize, err = statter.Stat(ctx, e.Content)
				if errors.Is(err, cas.ErrCapability) {
					lastSize, err = b.service.hashThrough(ctx, e.Content)
				}
			} else {
				lastSize, err = b.service.hashThrough(ctx, e.Content)
			}
			if err != nil {
				return fmt.Errorf("entry %q: %w", e.Path, err)
			}
			last = e.Content
		}
		if lastSize != e.Size {
			return invalidf("size mismatch for %q", e.Path)
		}
		return nil
	}
}

// Publish commits the manifest and registers it. A retry after failure is safe and
// converges on one artifact: the ID is a pure function of entries and metadata.
func (b *Builder) Publish(ctx context.Context) (ID, error) {
	if b.closed {
		return "", &PublishError{Err: errors.New("artifacts: builder closed")}
	}
	return b.service.commit(ctx, publication{
		metadata: b.options.Metadata,
		each: func(visit func(Entry) error) error {
			return b.sorter.Each(func(record []byte) error {
				_, encoded, _ := bytes.Cut(record, []byte{0})
				var e Entry
				if err := json.Unmarshal(encoded, &e); err != nil {
					return err
				}
				return visit(e)
			})
		},
		verifyContent: b.verifier(),
	})
}

// Close discards spill files.
func (b *Builder) Close() error {
	b.closed = true
	return b.sorter.Close()
}
