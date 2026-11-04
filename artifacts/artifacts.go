// Package artifacts publishes immutable canonical manifests over CAS and owns
// their authoritative catalog membership. It does not assign revision history.
package artifacts

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/craggy-io/artifact-services/cas"
	"github.com/craggy-io/artifact-services/internal/journal"
	"github.com/craggy-io/artifact-services/observability"
)

// ID identifies an immutable snapshot: the canonical manifest's CAS digest. Equal
// manifests have one ID. An Occurrence distinguishes separate publications of it.
type ID string

// EntryKind distinguishes manifest entries. The empty kind is a regular file.
type EntryKind string

const (
	// KindDir is an explicit directory, which lets an empty one exist.
	KindDir EntryKind = "dir"
	// KindLink embeds another registered artifact, or a subtree or file of one,
	// without copying its entries. Identity pins the target exactly, and a target
	// must exist before anything links to it, so links cannot form cycles.
	KindLink EntryKind = "link"
)

// Entry is one manifest member. Mode holds Unix permission bits (at most 07777)
// and zero means unspecified. Regular files set Content and Size. Links set Target
// and optionally TargetPath. Other fields must be zero for that kind.
type Entry struct {
	Path       string    `json:"path"`
	Content    cas.ID    `json:"content,omitempty"`
	Size       int64     `json:"size"`
	Mode       uint32    `json:"mode,omitempty"`
	Kind       EntryKind `json:"kind,omitempty"`
	Target     ID        `json:"target,omitempty"`
	TargetPath string    `json:"target_path,omitempty"`
}

// IsFile reports a regular file.
func (e Entry) IsFile() bool { return e.Kind == "" }

// Manifest is the logical snapshot description, convenient for small trees. For
// large trees use Builder and ListFiles, which never hold all entries in memory.
type Manifest struct {
	Version  int                        `json:"version"`
	Entries  []Entry                    `json:"entries"`
	Metadata map[string]json.RawMessage `json:"metadata,omitempty"`
}

var (
	ErrInvalidManifest = errors.New("artifacts: invalid manifest")
	ErrInvalidQuery    = errors.New("artifacts: invalid query")
	ErrNotFound        = errors.New("artifacts: not found")
	ErrConflict        = errors.New("artifacts: conflicting registration")
	ErrUnvalidated     = errors.New("artifacts: validated claim not registered")
	ErrClosed          = errors.New("artifacts: service closed")
	ErrCursor          = errors.New("artifacts: invalid, expired or mismatched cursor")
	ErrLimit           = errors.New("artifacts: resource limit exceeded")
	// ErrNotFile reports opening a directory or link entry as file content.
	ErrNotFile = errors.New("artifacts: entry is not a regular file")
	// ErrLeaseHeld reports another live owner of the catalog root.
	ErrLeaseHeld = journal.ErrLeaseHeld
)

// LeaseError carries the diagnostic identity of the live owner of a root.
type LeaseError = journal.LeaseError

// PublishError distinguishes immutable manifest availability from acceptance in
// the catalog. A nonempty returned ID alone is not proof either stage succeeded.
type PublishError struct {
	ID                ID
	ManifestCommitted bool
	Registered        bool
	Err               error
}

func (e *PublishError) Error() string {
	return fmt.Sprintf("artifacts: publish %s (manifest=%t registered=%t): %v", e.ID, e.ManifestCommitted, e.Registered, e.Err)
}
func (e *PublishError) Unwrap() error { return e.Err }

type Store interface {
	Publish(context.Context, Manifest) (ID, error)
	Get(context.Context, ID) (Manifest, error)
	OpenFile(context.Context, ID, string) (io.ReadCloser, error)
	Stat(context.Context, ID, string) (FileInfo, error)
	ListFiles(context.Context, ID, FileQuery) (FilePage, error)
	Search(context.Context, CatalogQuery) (CatalogPage, error)
	Category(context.Context, ID) (observability.Category, error)
	RecordOccurrence(context.Context, OccurrenceRequest) (Occurrence, error)
	GetOccurrence(context.Context, OccurrenceID) (Occurrence, error)
	Occurrences(context.Context, OccurrenceQuery) (OccurrencePage, error)
	StatContent(context.Context, cas.ID) (int64, error)
}

// FileInfo describes an entry resolved through any links, without reading content.
type FileInfo struct {
	Path    string
	Kind    EntryKind
	Content cas.ID
	Size    int64
	Mode    uint32
}

// FileQuery pages entries in path order. Expand replaces each link with the entries
// it embeds, under the link's path. Cursors are bound to the query and to this
// service instance, and do not expire while it lives because manifests are immutable.
type FileQuery struct {
	Prefix string
	Expand bool
	Limit  int
	Cursor string
}
type FilePage struct {
	Entries    []Entry
	NextCursor string
}

// Metadata predicates are ANDed exact typed scalar equality over configured keys.
// Zero Limit selects 100; the maximum is 1000. Cursors bind all query fields.
type CatalogQuery struct {
	ID                                     ID
	SchemaID                               ID
	Content                                cas.ID
	MinFiles, MaxFiles, MinBytes, MaxBytes *int64
	Metadata                               map[string]json.RawMessage
	IncludeInternal                        bool
	Limit                                  int
	Cursor                                 string
}

// CatalogItem summarizes a registered snapshot. Content is its content identity:
// for one regular file the file's digest, whatever its name or mode; otherwise a
// digest over the whole entry list, independent of manifest metadata. FileCount
// and LogicalBytes count this manifest's own regular files, not embedded ones.
type CatalogItem struct {
	ID           ID                     `json:"id"`
	FileCount    int64                  `json:"file_count"`
	LogicalBytes int64                  `json:"logical_bytes"`
	SchemaID     ID                     `json:"schema_id,omitempty"`
	Content      cas.ID                 `json:"content"`
	Category     observability.Category `json:"category"`
	Sequence     uint64                 `json:"sequence,omitempty"`
}
type CatalogPage struct {
	Items      []CatalogItem
	NextCursor string
}

// ParseID validates artifact IDs, which identify the canonical manifest in CAS.
func ParseID(value string) (ID, error) { id, err := cas.ParseID(value); return ID(id), err }
