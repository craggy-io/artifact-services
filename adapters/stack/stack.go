// Package stack opens every layer over one directory with the wiring that is easy to
// get wrong: distinct roots, cross-layer reference validators, and close order. It
// composes public constructors only; an application that needs fewer layers, or other
// paths, should construct them directly.
package stack

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/craggy-io/artifact-services/artifacts"
	"github.com/craggy-io/artifact-services/assertions"
	"github.com/craggy-io/artifact-services/cas"
	"github.com/craggy-io/artifact-services/lineage"
	"github.com/craggy-io/artifact-services/objects"
	"github.com/craggy-io/artifact-services/storage"
)

// Options configures a stack. Root holds everything. Authority and
// ReservedNamespaces go to the assertion layer; the rest narrow what each layer
// indexes or holds. Relaxed trades OS-crash durability for speed in every journal.
type Options struct {
	Root               string
	Authority          *assertions.Authority
	ReservedNamespaces map[string][]string
	ArtifactMetadata   []string
	LineageMetadata    []string
	MaxArtifactEntries int64
	MaxRecords         int
	MaxBytes           int64
	Objects            objects.Options
	Relaxed            bool
}

// Stack holds the open services. Blobs is the storage backend under CAS, exposed for
// tools that need its optional capabilities.
type Stack struct {
	Root       string
	Blobs      *storage.Filesystem
	CAS        *cas.Service
	Artifacts  *artifacts.Service
	Objects    *objects.Service
	Lineage    *lineage.Service
	Assertions *assertions.Service
}

// Open constructs the layers in dependency order. Reopening a root is how crash
// recovery happens: a dead owner's lease is already gone, and torn journal tails are
// truncated on open.
func Open(o Options) (*Stack, error) {
	blobs, err := storage.NewFilesystem(filepath.Join(o.Root, "blobs"))
	if err != nil {
		return nil, err
	}
	staging := filepath.Join(o.Root, "staging")
	if err := os.MkdirAll(staging, 0o755); err != nil {
		return nil, err
	}
	content, err := cas.New(blobs, cas.Options{TempDir: staging})
	if err != nil {
		return nil, err
	}
	snapshots, err := artifacts.New(content, artifacts.Options{
		Root:            filepath.Join(o.Root, "catalog"),
		IndexedMetadata: o.ArtifactMetadata,
		MaxEntries:      o.MaxArtifactEntries,
		MaxRecords:      o.MaxRecords,
		MaxBytes:        o.MaxBytes,
		Relaxed:         o.Relaxed,
		TempDir:         staging,
	})
	if err != nil {
		return nil, err
	}
	objectOptions := o.Objects
	if objectOptions.TempDir == "" {
		objectOptions.TempDir = staging
	}
	typed, err := objects.New(content, snapshots, objectOptions)
	if err != nil {
		return nil, errors.Join(err, snapshots.Close())
	}
	// Lineage and assertions can each reference the other's records, so one of them
	// must late-bind: the closure only runs once both exist.
	var claims *assertions.Service
	history, err := lineage.New(snapshots, lineage.Options{
		Root:             filepath.Join(o.Root, "lineage"),
		ValidateSelector: typed.ValidateRef,
		ValidateAssertion: func(ctx context.Context, id string) error {
			return claims.ValidateAssertion(ctx, id)
		},
		IndexedMetadata: o.LineageMetadata,
		MaxRecords:      o.MaxRecords,
		MaxBytes:        o.MaxBytes,
		Relaxed:         o.Relaxed,
	})
	if err != nil {
		return nil, errors.Join(err, snapshots.Close())
	}
	claims, err = assertions.New(snapshots, assertions.Options{
		Root:               filepath.Join(o.Root, "assertions"),
		Authority:          o.Authority,
		ReservedNamespaces: o.ReservedNamespaces,
		ValidateSelector:   typed.ValidateRef,
		ValidateRevision:   history.ValidateRevision,
		ValidateActivity:   history.ValidateActivity,
		MaxRecords:         o.MaxRecords,
		MaxBytes:           o.MaxBytes,
		Relaxed:            o.Relaxed,
	})
	if err != nil {
		return nil, errors.Join(err, history.Close(), snapshots.Close())
	}
	return &Stack{Root: o.Root, Blobs: blobs, CAS: content, Artifacts: snapshots, Objects: typed, Lineage: history, Assertions: claims}, nil
}

// Close releases the layers that hold leases, dependents first. Stop work and close
// streams beforehand. It retries nothing itself, but every layer's Close is safe to
// call again if this returns an error.
func (s *Stack) Close() error {
	return errors.Join(s.Assertions.Close(), s.Lineage.Close(), s.Artifacts.Close())
}
