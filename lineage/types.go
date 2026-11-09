// Package lineage stores generic immutable provenance and revision history.
package lineage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/craggy-io/artifact-services/artifacts"
	"github.com/craggy-io/artifact-services/backend"
	"github.com/craggy-io/artifact-services/internal/journal"
	"github.com/craggy-io/artifact-services/provenance"
)

type ID string

var (
	ErrInvalid             = errors.New("lineage: invalid record or query")
	ErrNotFound            = errors.New("lineage: record not found")
	ErrConflict            = errors.New("lineage: conflicting record identity")
	ErrUnsupportedSelector = errors.New("lineage: detailed selector validator required")
	ErrLimit               = errors.New("lineage: evidence or traversal work limit")
	ErrClosed              = errors.New("lineage: service closed")
	ErrCursor              = errors.New("lineage: invalid, expired or mismatched cursor")
	// ErrHeadConflict reports a guarded history that did not have the expected head.
	// It also matches ErrConflict, and the detail is in HeadConflictError.
	ErrHeadConflict = errors.New("lineage: history head moved")
	// ErrLeaseHeld reports another live owner of the journal root.
	ErrLeaseHeld = journal.ErrLeaseHeld
)

// LeaseError carries the diagnostic identity of the live owner of a root.
type LeaseError = journal.LeaseError

// HeadConflictError reports what a guarded append expected and what it found.
type HeadConflictError struct {
	History          string
	Expected, Actual ID
}

func (e *HeadConflictError) Error() string {
	return fmt.Sprintf("lineage: history %s head is %q, expected %q", e.History, e.Actual, e.Expected)
}

// Is matches both ErrHeadConflict and the general ErrConflict.
func (e *HeadConflictError) Is(target error) bool {
	return target == ErrHeadConflict || target == ErrConflict
}

type RecordError struct {
	ID        ID
	Committed bool
	Err       error
}

func (e *RecordError) Error() string {
	if e == nil || e.Err == nil {
		return "lineage: record"
	}
	return "lineage: record: " + e.Err.Error()
}
func (e *RecordError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

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

// Revision is one immutable history node. A History makes it part of a guarded
// linear history: its one parent must be that history's head when it commits, or
// no parent for the first revision. Without a History, Parents are free-form and
// may branch, and Head does not apply.
type Revision struct {
	Artifact artifacts.ID               `json:"artifact"`
	Parents  []ID                       `json:"parents,omitempty"`
	History  string                     `json:"history,omitempty"`
	Activity ID                         `json:"activity,omitempty"`
	Metadata map[string]json.RawMessage `json:"metadata,omitempty"`
}

// Record is one journal record. Sequence is its commit position, set on reads and
// ignored on writes, so a record read back can be replayed unchanged.
type Record struct {
	ID           ID            `json:"id"`
	Activity     *Activity     `json:"activity,omitempty"`
	Relationship *Relationship `json:"relationship,omitempty"`
	Revision     *Revision     `json:"revision,omitempty"`
	Sequence     uint64        `json:"sequence,omitempty"`
}
type Store interface {
	Record(context.Context, Record) (ID, error)
	Get(context.Context, ID) (Record, error)
	List(context.Context, Query) (Page, error)
	Traverse(context.Context, TraversalQuery) (TraversalPage, error)
}

// Options binds one exclusive journal root and its persisted index policy. A
// reference form whose validator is missing is rejected, never stored unchecked.
// IndexedMetadata names metadata keys answerable by Query.Metadata as an index hit;
// like the artifact catalog's, it is persisted and cannot change for a root.
// MaxRecords and MaxBytes bound the journal, zero selecting defaults. Relaxed skips
// the per-commit fsync: commits then survive process death but not OS crash.
type Options struct {
	Root              string
	ValidateSelector  func(context.Context, provenance.Ref) error
	ValidateAssertion func(context.Context, string) error
	IndexedMetadata   []string
	MaxRecords        int
	MaxBytes          int64
	Relaxed           bool
	// Journal selects the record log; nil keeps the local file log at Root.
	Journal backend.Opener
}

// Query predicates are ANDed. Role requires Ref. An empty role matches any
// reference position, including mapping endpoints; DependencyRole filters evidence.
// Metadata is exact scalar equality over keys in Options.IndexedMetadata, and with
// History it is an index hit rather than a journal scan.
type Query struct {
	Kind             string                     `json:"kind,omitempty"`
	ActivityKind     string                     `json:"activity_kind,omitempty"`
	RelationshipKind string                     `json:"relationship_kind,omitempty"`
	Ref              *provenance.Ref            `json:"ref,omitempty"`
	Role             string                     `json:"role,omitempty"`
	DependencyRole   string                     `json:"dependency_role,omitempty"`
	Activity         ID                         `json:"activity,omitempty"`
	History          string                     `json:"history,omitempty"`
	Metadata         map[string]json.RawMessage `json:"metadata,omitempty"`
	IncludeInternal  bool                       `json:"include_internal,omitempty"`
	Limit            int                        `json:"limit,omitempty"`
	Cursor           string                     `json:"-"`
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
