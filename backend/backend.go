// Package backend defines the seam beneath every journal: the authoritative,
// append-only record log that the artifact, lineage and assertion layers commit to.
//
// The default log is a local file under an exclusive OS lease. A shared log, such as
// adapters/postgres, lets several processes commit to one root: each commit is
// serialised by the log, and every process catches its in-memory indexes up to the
// log's latest sequence before it answers a read. See docs/durability.md.
package backend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

var (
	// ErrConflict reports a key already committed with different data.
	ErrConflict = errors.New("journal: conflicting identity")
	// ErrClosed reports a log that has been closed.
	ErrClosed = errors.New("journal: closed")
	// ErrCapacity reports a log at one of its configured limits.
	ErrCapacity = errors.New("journal: capacity exceeded")
	// ErrFormat reports persisted state this version cannot read.
	ErrFormat = errors.New("journal: unsupported on-disk format")
	// ErrCorrupt reports damaged persisted state.
	ErrCorrupt = errors.New("journal: corrupt log")
	// ErrConfig reports a root already bound to a different layer policy.
	ErrConfig = errors.New("journal: configuration differs from persisted policy")
)

// Entry is one committed record. Sequence is contiguous from 1 per root and equals
// commit order.
type Entry struct {
	Sequence uint64          `json:"sequence"`
	Key      string          `json:"key"`
	Data     json.RawMessage `json:"data"`
}

// CommitError reports a failed Append. Committed is true only when the log knows
// the record is durable despite the error.
type CommitError struct {
	Committed bool
	Err       error
}

func (e *CommitError) Error() string {
	return fmt.Sprintf("journal committed=%t: %v", e.Committed, e.Err)
}
func (e *CommitError) Unwrap() error { return e.Err }

// Log is one journal root's record log. Implementations must be safe for concurrent
// use. Data passed to Append is canonical JSON and must be returned byte for byte.
type Log interface {
	// Latest returns the newest committed sequence; zero means empty.
	Latest(ctx context.Context) (uint64, error)
	// Get returns a record by key. A missing key is (zero, false, nil).
	Get(ctx context.Context, key string) (Entry, bool, error)
	// At returns the record with an exact sequence, or ErrCorrupt if there is none.
	At(ctx context.Context, sequence uint64) (Entry, error)
	// Scan visits records with after < Sequence <= until in order. until must not
	// exceed a value Latest has returned.
	Scan(ctx context.Context, after, until uint64, visit func(Entry) error) error
	// Append commits one record inside the log's exclusive commit section. If the
	// key already exists, it returns that record and true without calling prepare.
	// Otherwise it calls prepare with the latest committed sequence, aborts if
	// prepare fails, and writes the record at latest+1. prepare's context lets
	// reads made through this Log during prepare share the commit section (a
	// shared log reads inside its open transaction). On an error the record is
	// absent unless the error is a CommitError with Committed set.
	Append(ctx context.Context, key string, data []byte, prepare func(ctx context.Context, latest uint64) error) (Entry, bool, error)
	// Watch calls hook, coalesced and on any goroutine, after another process
	// commits to this root. A log with a single writer never calls it.
	Watch(hook func()) (stop func())
	// Close releases the log. It is retryable.
	Close() error
}

// Opener opens the log for one journal root. config is the owning layer's
// canonical policy: a root already bound to different bytes must fail with
// ErrConfig, and a new root is bound to it.
type Opener func(ctx context.Context, config []byte) (Log, error)

// Journals names one Opener per journal root of a stack: "catalog", "lineage" and
// "assertions".
type Journals func(root string) Opener
