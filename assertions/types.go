// Package assertions records typed, immutable statements about stored things, and
// derives which of them are current. Kinds are registered at runtime with a schema
// and, optionally, the writer identities allowed to use them; what a kind or an
// identity means is entirely the caller's.
//
// Assertions are not artifacts and not lineage: their journal is the only source of
// current state, so nothing written to another layer can produce or alter one.
package assertions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/craggy-io/artifact-services/backend"
	"github.com/craggy-io/artifact-services/internal/journal"
	"github.com/craggy-io/artifact-services/objects"
	"github.com/craggy-io/artifact-services/observability"
	"github.com/craggy-io/artifact-services/provenance"
)

var (
	ErrInvalid  = errors.New("assertions: invalid record")
	ErrNotFound = errors.New("assertions: not found")
	// ErrConflict reports a commit the current state forbids: a key already held, a
	// replaced or retracted target, or an ID reused with different content.
	ErrConflict = errors.New("assertions: conflict")
	ErrClosed   = errors.New("assertions: closed")
	ErrLimit    = errors.New("assertions: resource limit")
	ErrCursor   = errors.New("assertions: invalid, expired or mismatched cursor")
	// ErrWriterForbidden reports a write without a credential the kind accepts.
	ErrWriterForbidden = errors.New("assertions: writer forbidden")
	// ErrNamespace reports a kind name inside a namespace the writer may not register.
	ErrNamespace = errors.New("assertions: reserved namespace")
	// ErrLeaseHeld reports another live owner of the journal root.
	ErrLeaseHeld = journal.ErrLeaseHeld
)

// LeaseError carries the diagnostic identity of the live owner of a root.
type LeaseError = journal.LeaseError

// ID identifies one assertion across replacement: each replacement has its own ID.
type ID string

// KindID is a registered kind's identity, "name@version".
type KindID string

// Kind declares a family of assertions. All fields are fixed once registered; a
// change is a new Version. Keyed kinds require a key on every assertion and allow at
// most one current assertion per key. Writers, if any, are the only identities that
// may assert, replace or retract this kind.
type Kind struct {
	Name    string        `json:"name"`
	Version int           `json:"version"`
	Shape   objects.Shape `json:"shape"`
	Keyed   bool          `json:"keyed,omitempty"`
	Writers []string      `json:"writers,omitempty"`
}

// ID returns the identity assertions use to name this kind.
func (k Kind) ID() KindID { return KindID(fmt.Sprintf("%s@%d", k.Name, k.Version)) }

// Request is one assertion to record. An empty ID is minted, but a retry can only be
// recognised as the same assertion if the caller chose the ID. Actor is a free-form
// label the caller supplies and nothing checks; only Writer is authenticated.
type Request struct {
	ID       ID
	Kind     KindID
	Subject  provenance.Ref
	Value    json.RawMessage
	Evidence []provenance.Ref
	Key      string
	Replaces ID
	Activity string
	Actor    string
	Writer   Writer
}

// Retraction ends an assertion's currency without replacing it. Retracting twice
// with identical fields is idempotent.
type Retraction struct {
	Assertion ID
	Activity  string
	Actor     string
	Writer    Writer
}

// Assertion is one recorded statement. Writer is the authenticated identity, empty
// when the kind accepts anyone. Current, EndedAt, EndedBy and Retracted are derived,
// and reflect the query's AsOf where one was given.
type Assertion struct {
	ID       ID                     `json:"id"`
	Kind     KindID                 `json:"kind"`
	Subject  provenance.Ref         `json:"subject"`
	Value    json.RawMessage        `json:"value"`
	Evidence []provenance.Ref       `json:"evidence,omitempty"`
	Key      string                 `json:"key,omitempty"`
	Replaces ID                     `json:"replaces,omitempty"`
	Activity string                 `json:"activity,omitempty"`
	Actor    string                 `json:"actor,omitempty"`
	Writer   string                 `json:"writer,omitempty"`
	Category observability.Category `json:"category,omitempty"`

	Sequence  uint64 `json:"-"`
	Current   bool   `json:"-"`
	EndedAt   uint64 `json:"-"`
	EndedBy   ID     `json:"-"`
	Retracted bool   `json:"-"`
}

// Query selects assertions, ANDing the predicates that are set. Exactly-equal refs
// match Subject and Evidence: an Occurrence subject never matches a Digest subject
// unless IncludeContent asks for assertions on the occurrence's content as well.
// Key requires Kind. By default only current assertions match; IncludeEnded adds
// replaced and retracted ones. AsOf evaluates all of it at a past sequence.
type Query struct {
	Kind            KindID          `json:"kind,omitempty"`
	Subject         *provenance.Ref `json:"subject,omitempty"`
	IncludeContent  bool            `json:"include_content,omitempty"`
	Key             string          `json:"key,omitempty"`
	Evidence        *provenance.Ref `json:"evidence,omitempty"`
	IncludeEnded    bool            `json:"include_ended,omitempty"`
	AsOf            uint64          `json:"as_of,omitempty"`
	IncludeInternal bool            `json:"include_internal,omitempty"`
	Limit           int             `json:"limit,omitempty"`
	Cursor          string          `json:"cursor,omitempty"`
}

// Page is one page of a query, in commit order, from a snapshot fixed by the first
// call. Records committed later do not enter its remaining pages.
type Page struct {
	Assertions []Assertion
	NextCursor string
}

// RecordError reports how far a write got. Committed means the record is durable
// even though the call returned an error; retrying with the same ID is safe.
type RecordError struct {
	ID        ID
	Committed bool
	Err       error
}

func (e *RecordError) Error() string {
	return fmt.Sprintf("assertions: record %s (committed=%t): %v", e.ID, e.Committed, e.Err)
}
func (e *RecordError) Unwrap() error { return e.Err }

// Options binds one exclusive journal root.
//
// Authority verifies credentials; without one, every kind that names Writers is
// unusable and cannot be registered. ReservedNamespaces maps a kind-name prefix to
// the identities allowed to register kinds under it, which stops anyone else
// claiming a name, or a new version of one, before its rightful owner does.
//
// ValidateSelector, ValidateRevision and ValidateActivity check targets this layer
// cannot reach itself. A reference form whose validator is missing is rejected, not
// stored unchecked. MaxRecords and MaxBytes bound the journal, zero selecting
// defaults. Relaxed skips the per-commit fsync: commits then survive process death
// but not OS crash.
type Options struct {
	Root               string
	Authority          *Authority
	ReservedNamespaces map[string][]string
	ValidateSelector   func(ctx context.Context, ref provenance.Ref) error
	ValidateRevision   func(ctx context.Context, revision, artifact string) error
	ValidateActivity   func(ctx context.Context, activity string) error
	MaxRecords         int
	MaxBytes           int64
	Relaxed            bool
	// Journal selects the record log; nil keeps the local file log at Root.
	Journal backend.Opener
}
