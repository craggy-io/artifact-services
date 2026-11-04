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
	"github.com/craggy-io/artifact-services/observability"
)

type ID string

type Entry struct {
	Path    string `json:"path"`
	Content cas.ID `json:"content"`
	Size    int64  `json:"size"`
}
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
)

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
	ListFiles(context.Context, ID, FileQuery) (FilePage, error)
	Search(context.Context, CatalogQuery) (CatalogPage, error)
	Category(context.Context, ID) (observability.Category, error)
}

type FileQuery struct {
	Prefix string
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
	MinFiles, MaxFiles, MinBytes, MaxBytes *int64
	Metadata                               map[string]json.RawMessage
	IncludeInternal                        bool
	Limit                                  int
	Cursor                                 string
}
type CatalogItem struct {
	ID           ID                     `json:"id"`
	FileCount    int64                  `json:"file_count"`
	LogicalBytes int64                  `json:"logical_bytes"`
	SchemaID     ID                     `json:"schema_id,omitempty"`
	Category     observability.Category `json:"category"`
}
type CatalogPage struct {
	Items      []CatalogItem
	NextCursor string
}

// ParseID validates artifact IDs, which identify the canonical manifest in CAS.
func ParseID(value string) (ID, error) { id, err := cas.ParseID(value); return ID(id), err }
