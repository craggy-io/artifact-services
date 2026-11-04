// Package lineage stores generic immutable provenance and revision history.
package lineage

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/craggy-io/artifact-services/artifacts"
	"github.com/craggy-io/artifact-services/provenance"
)

type ID string

var (
	ErrInvalid             = errors.New("lineage: invalid record or query")
	ErrNotFound            = errors.New("lineage: record not found")
	ErrConflict            = errors.New("lineage: conflicting record identity")
	ErrUnsupportedSelector = errors.New("lineage: detailed selector validator required")
	ErrLimit               = errors.New("lineage: evidence or traversal work limit")
)

type RecordError struct {
	ID        ID
	Committed bool
	Err       error
}

func (e *RecordError) Error() string { return "lineage: record: " + e.Err.Error() }
func (e *RecordError) Unwrap() error { return e.Err }

// Evidence identifies immutable NDJSON provenance.Mapping records. Coverage must
// be declared explicitly. Supporting artifacts need not be visible in discovery.
type Evidence struct {
	Artifact artifacts.ID        `json:"artifact"`
	Path     string              `json:"path"`
	Coverage provenance.Coverage `json:"coverage"`
}
type Activity struct {
	Kind           string                     `json:"kind"`
	Inputs         []provenance.Ref           `json:"inputs,omitempty"`
	Outputs        []provenance.Ref           `json:"outputs,omitempty"`
	Actor          string                     `json:"actor,omitempty"`
	ExternalSource string                     `json:"external_source,omitempty"`
	Metadata       map[string]json.RawMessage `json:"metadata,omitempty"`
	Evidence       []Evidence                 `json:"evidence,omitempty"`
}
type Relationship struct {
	Kind     string                     `json:"kind"`
	From     provenance.Ref             `json:"from"`
	To       provenance.Ref             `json:"to"`
	Activity ID                         `json:"activity,omitempty"`
	Metadata map[string]json.RawMessage `json:"metadata,omitempty"`
	Evidence []Evidence                 `json:"evidence,omitempty"`
}
type Revision struct {
	Artifact artifacts.ID               `json:"artifact"`
	Parents  []ID                       `json:"parents,omitempty"`
	Activity ID                         `json:"activity,omitempty"`
	Metadata map[string]json.RawMessage `json:"metadata,omitempty"`
}
type Record struct {
	ID           ID            `json:"id"`
	Activity     *Activity     `json:"activity,omitempty"`
	Relationship *Relationship `json:"relationship,omitempty"`
	Revision     *Revision     `json:"revision,omitempty"`
}
type Store interface {
	Record(context.Context, Record) (ID, error)
	Get(context.Context, ID) (Record, error)
	List(context.Context, Query) (Page, error)
	Traverse(context.Context, TraversalQuery) (TraversalPage, error)
}
type Options struct {
	Root             string
	ValidateSelector func(context.Context, provenance.Ref) error
}

// Query predicates are ANDed. Role requires Ref. An empty role matches any
// reference position, including mapping endpoints; DependencyRole filters evidence.
type Query struct {
	Kind             string          `json:"kind,omitempty"`
	ActivityKind     string          `json:"activity_kind,omitempty"`
	RelationshipKind string          `json:"relationship_kind,omitempty"`
	Ref              *provenance.Ref `json:"ref,omitempty"`
	Role             string          `json:"role,omitempty"`
	DependencyRole   string          `json:"dependency_role,omitempty"`
	Activity         ID              `json:"activity,omitempty"`
	IncludeInternal  bool            `json:"include_internal,omitempty"`
	Limit            int             `json:"limit,omitempty"`
	Cursor           string          `json:"-"`
}
type Page struct {
	Records    []Record
	NextCursor string
}
type TraversalQuery struct {
	Root            provenance.Ref `json:"root"`
	Kinds           []string       `json:"kinds,omitempty"`
	Direction       string         `json:"direction,omitempty"`
	DependencyRole  string         `json:"dependency_role,omitempty"`
	MaxDepth        int            `json:"max_depth,omitempty"`
	MaxNodes        int            `json:"max_nodes,omitempty"`
	MaxResults      int            `json:"max_results,omitempty"`
	Limit           int            `json:"limit,omitempty"`
	IncludeInternal bool           `json:"include_internal,omitempty"`
	Cursor          string         `json:"-"`
}
type Node struct {
	Ref   provenance.Ref
	Depth int
}
type Edge struct {
	Expression     string
	ID             string
	Kind           string
	From, To       provenance.Ref
	Activity       ID
	DependencyRole string
	Evidence       *Evidence
}
type TraversalPage struct {
	Nodes      []Node
	Edges      []Edge
	NextCursor string
	Truncated  bool
	Reason     string
	Coverage   []provenance.Coverage
}
