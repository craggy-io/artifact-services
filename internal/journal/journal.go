// Package journal owns one append-only root of immutable records over a
// backend.Log, and the order in which a layer's in-memory state sees them.
//
// The layer keeps derived state (indexes, heads) in memory. Every record reaches
// that state exactly once and in sequence order: records this Store commits through
// Txn.Apply, and records it did not commit (all of them on open, and those another
// process committed to a shared log) through Options.Follow. CatchUp applies the
// latter, so a layer that calls it before answering a read sees every commit that
// finished before the read began, whichever process made it.
package journal

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"unicode/utf8"

	"github.com/craggy-io/artifact-services/backend"
	"github.com/craggy-io/artifact-services/internal/canon"
	"github.com/craggy-io/artifact-services/internal/crashpoint"
)

var (
	ErrConflict = backend.ErrConflict
	ErrClosed   = backend.ErrClosed
	ErrCapacity = backend.ErrCapacity
	ErrFormat   = backend.ErrFormat
	ErrCorrupt  = backend.ErrCorrupt
	ErrConfig   = backend.ErrConfig
)

const maxRecord = 8 << 20

// Entry is one committed record. Sequence is contiguous from 1 per root.
type Entry = backend.Entry

// CommitError reports a failed commit; see backend.CommitError.
type CommitError = backend.CommitError

// Options configures one root. Log selects a backend; nil selects the local file
// log at the root passed to Open, which MaxBytes, MaxRecords and Relaxed configure.
// Relaxed skips the per-commit fsync: commits then survive process death but not OS
// crash or power loss. Follow applies a record this Store did not commit to the
// layer's derived state; it runs with commits excluded and must not call Commit.
type Options struct {
	Config     []byte
	MaxBytes   int64
	MaxRecords int
	Relaxed    bool
	Log        backend.Opener
	Follow     func(Entry) error
}

// Txn is one record plus hooks run inside the commit critical section. Check
// runs after the idempotency decision, so a replayed identical record reuses
// without being re-checked. Check sees derived state caught up to the log's latest
// record. Apply updates derived state once the record is durable; it must not fail.
// When Apply is nil, Follow applies the record. Both hooks run with commits
// excluded, so neither may call Commit or CatchUp.
type Txn struct {
	Key   string
	Data  []byte
	Check func() error
	Apply func(Entry)
}

// Store is safe for concurrent use. Lock order: Store's own lock is taken before
// any lock a Check, Apply or Follow hook takes, so callers must not hold those
// locks across Commit or CatchUp.
type Store struct {
	mu          sync.RWMutex
	log         backend.Log
	follow      func(Entry) error
	applied     uint64
	closed      atomic.Bool
	subscribers map[*subscriber]struct{}
	stopWatch   func()
}

// Open opens a root. It applies nothing yet: the caller stores the returned Store
// where Follow can reach it, then calls CatchUp to apply every committed record.
func Open(root string, o Options) (*Store, error) {
	config := o.Config
	if len(config) == 0 {
		config = []byte("{}")
	}
	config, err := canon.Normalize(config)
	if err != nil {
		return nil, err
	}
	var log backend.Log
	if o.Log != nil {
		log, err = o.Log(context.Background(), config)
	} else {
		log, err = openFile(root, o, config)
	}
	if err != nil {
		return nil, err
	}
	s := &Store{log: log, follow: o.Follow, subscribers: map[*subscriber]struct{}{}}
	s.stopWatch = log.Watch(s.remoteCommit)
	return s, nil
}

// remoteCommit applies another process's commits, then wakes subscribers.
func (s *Store) remoteCommit() {
	if s.closed.Load() {
		return
	}
	if err := s.CatchUp(context.Background()); err != nil {
		return // the next read retries, and reports, the same catch-up
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.notify(s.applied)
}

// CatchUp applies records committed after the last one this Store has applied.
// For the local file log it is one comparison; for a shared log it is one query
// plus the records it finds.
func (s *Store) CatchUp(ctx context.Context) error {
	if s.closed.Load() {
		return ErrClosed
	}
	latest, err := s.log.Latest(ctx)
	if err != nil {
		return err
	}
	s.mu.RLock()
	current := s.applied >= latest
	s.mu.RUnlock()
	if current {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.catchUpLocked(ctx, latest)
}

func (s *Store) catchUpLocked(ctx context.Context, latest uint64) error {
	if s.applied >= latest {
		return nil
	}
	return s.log.Scan(ctx, s.applied, latest, func(e Entry) error {
		if e.Sequence != s.applied+1 {
			return ErrCorrupt
		}
		if s.follow != nil {
			if err := s.follow(e); err != nil {
				return err
			}
		}
		s.applied = e.Sequence
		return nil
	})
}

// View runs fn with commits excluded, passing the latest applied sequence. It lets
// a caller swap derived state atomically against commits, in the same lock order.
func (s *Store) View(fn func(sequence uint64)) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	fn(s.applied)
}

// Sequence returns the latest applied sequence; zero means empty. Call CatchUp
// first to include other processes' commits.
func (s *Store) Sequence() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.applied
}

// Get returns a record by key. A missing key is (zero, false, nil). It takes no
// Store lock, so Follow may call it.
func (s *Store) Get(key string) (Entry, bool, error) {
	if s.closed.Load() {
		return Entry{}, false, ErrClosed
	}
	return s.log.Get(context.Background(), key)
}

// At returns the record with an exact sequence. It takes no Store lock.
func (s *Store) At(sequence uint64) (Entry, error) {
	if s.closed.Load() {
		return Entry{}, ErrClosed
	}
	return s.log.At(context.Background(), sequence)
}

// Scan visits records with after < Sequence <= until in order, one resident
// record at a time. An until of zero, or one past the applied sequence, means the
// applied sequence at call time, so concurrent commits never enter the scan.
func (s *Store) Scan(ctx context.Context, after, until uint64, visit func(Entry) error) error {
	if s.closed.Load() {
		return ErrClosed
	}
	applied := s.Sequence()
	if until == 0 || until > applied {
		until = applied
	}
	if after >= until {
		return nil
	}
	return s.log.Scan(ctx, after, until, visit)
}

// Append commits a record with no hooks. It is the common case of Commit.
func (s *Store) Append(ctx context.Context, key string, data []byte) (Entry, bool, error) {
	return s.Commit(ctx, Txn{Key: key, Data: data})
}

// Commit appends one record atomically. Reused reports that an identical record
// already held this key; a different record is ErrConflict. On any error the
// record is absent, including after a restart, unless the error is a CommitError
// with Committed set.
func (s *Store) Commit(ctx context.Context, t Txn) (Entry, bool, error) {
	if err := ctx.Err(); err != nil {
		return Entry{}, false, err
	}
	if err := requireKey(t.Key); err != nil {
		return Entry{}, false, err
	}
	if len(t.Data) > maxRecord {
		return Entry{}, false, errors.New("journal: record size limit")
	}
	canonical, err := canon.Normalize(t.Data)
	if err != nil {
		return Entry{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed.Load() {
		return Entry{}, false, ErrClosed
	}
	e, existing, err := s.log.Append(ctx, t.Key, canonical, func(ctx context.Context, latest uint64) error {
		if err := s.catchUpLocked(ctx, latest); err != nil {
			return err
		}
		if t.Check != nil {
			return t.Check()
		}
		return nil
	})
	if err != nil {
		return Entry{}, false, err
	}
	if existing {
		if !bytes.Equal(e.Data, canonical) {
			return Entry{}, false, ErrConflict
		}
		// Another process may have committed it: derived state must hold it too.
		return e, true, s.catchUpLocked(ctx, e.Sequence)
	}
	if t.Apply != nil {
		t.Apply(e)
	} else if s.follow != nil {
		if err := s.follow(e); err != nil {
			return e, false, &CommitError{Committed: true, Err: err}
		}
	}
	s.applied = e.Sequence
	crashpoint.Hit(crashpoint.JournalCommitted)
	s.notify(e.Sequence)
	return e, false, nil
}

// Close stops commits, releases the log and ends subscribers. It is retryable: a
// failed step is attempted again by a later Close.
func (s *Store) Close() error {
	s.closed.Store(true)
	s.mu.Lock()
	stopWatch := s.stopWatch
	s.stopWatch = nil
	for sub := range s.subscribers {
		sub.stop()
		delete(s.subscribers, sub)
	}
	s.mu.Unlock()
	if stopWatch != nil {
		stopWatch()
	}
	return s.log.Close()
}

func requireKey(key string) error {
	if key == "" || len(key) > 1024 || !utf8.ValidString(key) {
		return errors.New("journal: key limit")
	}
	return nil
}
