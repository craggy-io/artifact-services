// Package postgres is a shared journal backend: every journal root is a pair of rows
// in PostgreSQL, so any number of processes can serve one stack.
//
// Each commit is one transaction that locks its root's row, so commit order is
// sequence order and sequences are contiguous. A layer's guard (Txn.Check) runs
// inside that transaction after the process has caught up to the locked latest
// sequence, so of two racing guarded commits exactly one wins. After commit, a
// NOTIFY wakes the other processes, which catch up from their last sequence. See
// docs/durability.md in the artifact-services module.
//
// The caller owns the pool: connect it however the deployment requires (for Cloud
// SQL, the Cloud SQL Go connector with IAM database authentication) and close it
// after closing the stack. Size it for one held listener connection plus, per
// journal root, a commit transaction and a concurrent read: six is the minimum for a
// full stack.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/craggy-io/artifact-services/backend"
)

// Channel is the LISTEN/NOTIFY channel commits are announced on; the payload is
// the committed root's name.
const Channel = "artifact_journal"

const scanBatch = 512

const schema = `
CREATE TABLE IF NOT EXISTS artifact_journal_roots (
	root   text PRIMARY KEY,
	config bytea NOT NULL,
	latest bigint NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS artifact_journal_entries (
	root         text NOT NULL REFERENCES artifact_journal_roots (root),
	sequence     bigint NOT NULL,
	key          text NOT NULL,
	data         bytea NOT NULL,
	committed_at timestamptz NOT NULL DEFAULT now(),
	PRIMARY KEY (root, sequence),
	UNIQUE (root, key)
);`

// Options configures a backend. Prefix namespaces root names, so several stacks
// can share one database; roots are named Prefix + name.
type Options struct {
	Prefix string
}

// Backend opens journal roots in one database. It is safe for concurrent use.
type Backend struct {
	pool    *pgxpool.Pool
	options Options

	mu       sync.Mutex
	watchers map[string]map[*watcher]struct{}
	listen   context.CancelFunc
	stopped  chan struct{}
}

type watcher struct{ hook func() }

// New creates the schema if it is missing and returns a backend over pool.
func New(ctx context.Context, pool *pgxpool.Pool, o Options) (*Backend, error) {
	if pool == nil {
		return nil, errors.New("postgres: nil pool")
	}
	if err := migrate(ctx, pool); err != nil {
		return nil, fmt.Errorf("postgres: migrate: %w", err)
	}
	return &Backend{pool: pool, options: o, watchers: map[string]map[*watcher]struct{}{}}, nil
}

// migrate is safe to run from several processes at once: an advisory lock makes
// one of them create the tables while the others wait.
func migrate(ctx context.Context, pool *pgxpool.Pool) error {
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('artifact-services/journal-schema'))`); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, schema)
		return err
	})
}

// Journals returns one Opener per stack journal root, for stack.Options.Journals.
func (b *Backend) Journals() backend.Journals {
	return func(name string) backend.Opener { return b.Opener(name) }
}

// Opener opens one named root.
func (b *Backend) Opener(name string) backend.Opener {
	root := b.options.Prefix + name
	return func(ctx context.Context, config []byte) (backend.Log, error) {
		if root == "" {
			return nil, errors.New("postgres: empty root name")
		}
		if _, err := b.pool.Exec(ctx, `INSERT INTO artifact_journal_roots (root, config) VALUES ($1, $2) ON CONFLICT (root) DO NOTHING`, root, config); err != nil {
			return nil, err
		}
		var bound []byte
		if err := b.pool.QueryRow(ctx, `SELECT config FROM artifact_journal_roots WHERE root = $1`, root).Scan(&bound); err != nil {
			return nil, err
		}
		if string(bound) != string(config) {
			return nil, fmt.Errorf("%w: root %s", backend.ErrConfig, root)
		}
		return &log{backend: b, root: root, closed: make(chan struct{})}, nil
	}
}

// Close stops the listener. Close every stack using the backend first; the pool
// stays open.
func (b *Backend) Close() error {
	b.mu.Lock()
	cancel, stopped := b.listen, b.stopped
	b.listen, b.stopped = nil, nil
	b.watchers = map[string]map[*watcher]struct{}{}
	b.mu.Unlock()
	if cancel != nil {
		cancel()
		<-stopped
	}
	return nil
}

func (b *Backend) watch(root string, hook func()) func() {
	w := &watcher{hook: hook}
	b.mu.Lock()
	if b.watchers[root] == nil {
		b.watchers[root] = map[*watcher]struct{}{}
	}
	b.watchers[root][w] = struct{}{}
	if b.listen == nil {
		ctx, cancel := context.WithCancel(context.Background())
		b.listen, b.stopped = cancel, make(chan struct{})
		go b.listenLoop(ctx, b.stopped)
	}
	b.mu.Unlock()
	return func() {
		b.mu.Lock()
		delete(b.watchers[root], w)
		b.mu.Unlock()
	}
}

// wake calls the hooks for root, or for every root when root is empty (after a
// reconnect, when notifications may have been missed).
func (b *Backend) wake(root string) {
	b.mu.Lock()
	var hooks []func()
	for name, set := range b.watchers {
		if root != "" && name != root {
			continue
		}
		for w := range set {
			hooks = append(hooks, w.hook)
		}
	}
	b.mu.Unlock()
	for _, hook := range hooks {
		go hook()
	}
}

// listenLoop holds one connection in LISTEN, reconnecting with backoff. Every
// reconnect wakes all watchers, since commits may have happened while it was down.
func (b *Backend) listenLoop(ctx context.Context, stopped chan struct{}) {
	defer close(stopped)
	delay := 100 * time.Millisecond
	for ctx.Err() == nil {
		err := b.listenOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
			delay = min(delay*2, 10*time.Second)
		}
	}
}

func (b *Backend) listenOnce(ctx context.Context) error {
	conn, err := b.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "LISTEN "+Channel); err != nil {
		return err
	}
	b.wake("")
	for {
		n, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			// The connection's state is unknown after an interrupted wait.
			conn.Conn().PgConn().Close(context.Background())
			return err
		}
		b.wake(n.Payload)
	}
}

// log is one root.
type log struct {
	backend *Backend
	root    string
	once    sync.Once
	closed  chan struct{}
}

var _ backend.Log = (*log)(nil)

// querier is a pool or, inside Append's prepare, the open transaction.
type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

type txKey struct{ log *log }

func (l *log) q(ctx context.Context) querier {
	if tx, ok := ctx.Value(txKey{l}).(pgx.Tx); ok {
		return tx
	}
	return l.backend.pool
}

func (l *log) isClosed() bool {
	select {
	case <-l.closed:
		return true
	default:
		return false
	}
}

func (l *log) Latest(ctx context.Context) (uint64, error) {
	if l.isClosed() {
		return 0, backend.ErrClosed
	}
	var latest int64
	if err := l.q(ctx).QueryRow(ctx, `SELECT latest FROM artifact_journal_roots WHERE root = $1`, l.root).Scan(&latest); err != nil {
		return 0, err
	}
	return uint64(latest), nil
}

func (l *log) Get(ctx context.Context, key string) (backend.Entry, bool, error) {
	if l.isClosed() {
		return backend.Entry{}, false, backend.ErrClosed
	}
	e := backend.Entry{Key: key}
	var sequence int64
	err := l.q(ctx).QueryRow(ctx, `SELECT sequence, data FROM artifact_journal_entries WHERE root = $1 AND key = $2`, l.root, key).Scan(&sequence, (*[]byte)(&e.Data))
	if errors.Is(err, pgx.ErrNoRows) {
		return backend.Entry{}, false, nil
	}
	if err != nil {
		return backend.Entry{}, false, err
	}
	e.Sequence = uint64(sequence)
	return e, true, nil
}

func (l *log) At(ctx context.Context, sequence uint64) (backend.Entry, error) {
	if l.isClosed() {
		return backend.Entry{}, backend.ErrClosed
	}
	e := backend.Entry{Sequence: sequence}
	err := l.q(ctx).QueryRow(ctx, `SELECT key, data FROM artifact_journal_entries WHERE root = $1 AND sequence = $2`, l.root, int64(sequence)).Scan(&e.Key, (*[]byte)(&e.Data))
	if errors.Is(err, pgx.ErrNoRows) {
		return backend.Entry{}, fmt.Errorf("%w: no sequence %d", backend.ErrCorrupt, sequence)
	}
	return e, err
}

// Scan reads in batches and visits each batch after its rows are closed, so a
// visitor may query the log itself.
func (l *log) Scan(ctx context.Context, after, until uint64, visit func(backend.Entry) error) error {
	if l.isClosed() {
		return backend.ErrClosed
	}
	for after < until {
		rows, err := l.q(ctx).Query(ctx, `SELECT sequence, key, data FROM artifact_journal_entries
			WHERE root = $1 AND sequence > $2 AND sequence <= $3 ORDER BY sequence LIMIT $4`, l.root, int64(after), int64(until), scanBatch)
		if err != nil {
			return err
		}
		batch, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (backend.Entry, error) {
			var e backend.Entry
			var sequence int64
			err := row.Scan(&sequence, &e.Key, (*[]byte)(&e.Data))
			e.Sequence = uint64(sequence)
			return e, err
		})
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			return fmt.Errorf("%w: sequences %d..%d missing", backend.ErrCorrupt, after+1, until)
		}
		for _, e := range batch {
			if err := ctx.Err(); err != nil {
				return err
			}
			if e.Sequence != after+1 {
				return fmt.Errorf("%w: sequence %d at position %d", backend.ErrCorrupt, e.Sequence, after+1)
			}
			if err := visit(e); err != nil {
				return err
			}
			after = e.Sequence
		}
	}
	return nil
}

func (l *log) Append(ctx context.Context, key string, data []byte, prepare func(context.Context, uint64) error) (backend.Entry, bool, error) {
	if l.isClosed() {
		return backend.Entry{}, false, backend.ErrClosed
	}
	tx, err := l.backend.pool.Begin(ctx)
	if err != nil {
		return backend.Entry{}, false, err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	var latest int64
	if err := tx.QueryRow(ctx, `SELECT latest FROM artifact_journal_roots WHERE root = $1 FOR UPDATE`, l.root).Scan(&latest); err != nil {
		return backend.Entry{}, false, err
	}
	inTx := context.WithValue(ctx, txKey{l}, tx)
	if existing, ok, err := l.Get(inTx, key); err != nil || ok {
		return existing, ok, err
	}
	if err := prepare(inTx, uint64(latest)); err != nil {
		return backend.Entry{}, false, err
	}
	e := backend.Entry{Sequence: uint64(latest) + 1, Key: key, Data: data}
	if _, err := tx.Exec(ctx, `INSERT INTO artifact_journal_entries (root, sequence, key, data) VALUES ($1, $2, $3, $4)`, l.root, int64(e.Sequence), key, data); err != nil {
		return backend.Entry{}, false, err
	}
	if _, err := tx.Exec(ctx, `UPDATE artifact_journal_roots SET latest = $2 WHERE root = $1`, l.root, int64(e.Sequence)); err != nil {
		return backend.Entry{}, false, err
	}
	if _, err := tx.Exec(ctx, `SELECT pg_notify($1, $2)`, Channel, l.root); err != nil {
		return backend.Entry{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return l.resolve(e, err)
	}
	return e, false, nil
}

// resolve decides a failed COMMIT's outcome by reading the record back: a lost
// connection can fail the call after the server committed.
func (l *log) resolve(e backend.Entry, commitErr error) (backend.Entry, bool, error) {
	var pgErr *pgconn.PgError
	if errors.As(commitErr, &pgErr) {
		return backend.Entry{}, false, commitErr // the server refused: nothing committed
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	found, ok, err := l.Get(ctx, e.Key)
	switch {
	case err != nil:
		return backend.Entry{}, false, &backend.CommitError{Err: errors.Join(fmt.Errorf("postgres: commit outcome unknown: %w", commitErr), err)}
	case ok && found.Sequence == e.Sequence && string(found.Data) == string(e.Data):
		return found, false, nil
	default:
		return backend.Entry{}, false, &backend.CommitError{Err: commitErr}
	}
}

func (l *log) Watch(hook func()) func() {
	if l.isClosed() {
		return func() {}
	}
	return l.backend.watch(l.root, hook)
}

func (l *log) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}
