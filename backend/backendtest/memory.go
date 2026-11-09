package backendtest

import (
	"bytes"
	"context"
	"sync"

	"github.com/craggy-io/artifact-services/backend"
)

// Memory is a set of shared journals held in memory, for tests of code that runs as
// several processes over one shared log: every Opener that Open returns, and every call
// of it, is another process's handle on the same root, and a commit through one handle
// wakes the others' watchers. It passes Run with shared set. Pass m.Open as a
// backend.Journals.
type Memory struct {
	mu    sync.Mutex
	roots map[string]*memRoot
}

// NewMemory returns empty journals.
func NewMemory() *Memory { return &Memory{roots: map[string]*memRoot{}} }

// Open returns the Opener of journal root name.
func (m *Memory) Open(name string) backend.Opener {
	return func(ctx context.Context, config []byte) (backend.Log, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		m.mu.Lock()
		r := m.roots[name]
		if r == nil {
			r = &memRoot{config: bytes.Clone(config), keys: map[string]int{}, handles: map[*memLog]bool{}}
			m.roots[name] = r
		}
		m.mu.Unlock()
		return r.open(config)
	}
}

// Copy returns independent journals holding every record committed so far, as a
// database's point-in-time recovery would.
func (m *Memory) Copy() *Memory {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := NewMemory()
	for name, r := range m.roots {
		r.mu.Lock()
		c := &memRoot{config: bytes.Clone(r.config), keys: map[string]int{}, handles: map[*memLog]bool{}}
		for i, e := range r.entries {
			c.entries = append(c.entries, clone(e))
			c.keys[e.Key] = i
		}
		r.mu.Unlock()
		out.roots[name] = c
	}
	return out
}

type memRoot struct {
	commit sync.Mutex // the exclusive commit section

	mu      sync.Mutex
	config  []byte
	entries []backend.Entry
	keys    map[string]int
	handles map[*memLog]bool
}

func (r *memRoot) open(config []byte) (backend.Log, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !bytes.Equal(r.config, config) {
		return nil, backend.ErrConfig
	}
	l := &memLog{root: r, hooks: map[int]func(){}}
	r.handles[l] = true
	return l, nil
}

type memLog struct {
	root   *memRoot
	closed bool // guarded by root.mu
	hooks  map[int]func()
	next   int
}

func (l *memLog) lock() (*memRoot, error) {
	l.root.mu.Lock()
	if l.closed {
		l.root.mu.Unlock()
		return nil, backend.ErrClosed
	}
	return l.root, nil
}

func (l *memLog) Latest(ctx context.Context) (uint64, error) {
	r, err := l.lock()
	if err != nil {
		return 0, err
	}
	defer r.mu.Unlock()
	return uint64(len(r.entries)), ctx.Err()
}

func (l *memLog) Get(ctx context.Context, key string) (backend.Entry, bool, error) {
	r, err := l.lock()
	if err != nil {
		return backend.Entry{}, false, err
	}
	defer r.mu.Unlock()
	i, ok := r.keys[key]
	if !ok {
		return backend.Entry{}, false, ctx.Err()
	}
	return clone(r.entries[i]), true, ctx.Err()
}

func (l *memLog) At(ctx context.Context, sequence uint64) (backend.Entry, error) {
	r, err := l.lock()
	if err != nil {
		return backend.Entry{}, err
	}
	defer r.mu.Unlock()
	if sequence == 0 || sequence > uint64(len(r.entries)) {
		return backend.Entry{}, backend.ErrCorrupt
	}
	return clone(r.entries[sequence-1]), ctx.Err()
}

func (l *memLog) Scan(ctx context.Context, after, until uint64, visit func(backend.Entry) error) error {
	r, err := l.lock()
	if err != nil {
		return err
	}
	until = min(until, uint64(len(r.entries)))
	var batch []backend.Entry
	for s := after + 1; s <= until; s++ {
		batch = append(batch, clone(r.entries[s-1]))
	}
	r.mu.Unlock()
	for _, e := range batch {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(e); err != nil {
			return err
		}
	}
	return nil
}

func (l *memLog) Append(ctx context.Context, key string, data []byte, prepare func(context.Context, uint64) error) (backend.Entry, bool, error) {
	l.root.commit.Lock()
	defer l.root.commit.Unlock()
	if e, ok, err := l.Get(ctx, key); err != nil || ok {
		return e, ok, err
	}
	latest, err := l.Latest(ctx)
	if err != nil {
		return backend.Entry{}, false, err
	}
	if err := prepare(ctx, latest); err != nil {
		return backend.Entry{}, false, err
	}
	r, err := l.lock()
	if err != nil {
		return backend.Entry{}, false, err
	}
	e := backend.Entry{Sequence: latest + 1, Key: key, Data: bytes.Clone(data)}
	r.keys[key] = len(r.entries)
	r.entries = append(r.entries, e)
	var wake []func()
	for h := range r.handles {
		if h != l {
			for _, hook := range h.hooks {
				wake = append(wake, hook)
			}
		}
	}
	r.mu.Unlock()
	for _, hook := range wake {
		go hook()
	}
	return clone(e), false, nil
}

func (l *memLog) Watch(hook func()) (stop func()) {
	r := l.root
	r.mu.Lock()
	id := l.next
	l.next++
	l.hooks[id] = hook
	r.mu.Unlock()
	return func() {
		r.mu.Lock()
		delete(l.hooks, id)
		r.mu.Unlock()
	}
}

func (l *memLog) Close() error {
	r := l.root
	r.mu.Lock()
	l.closed = true
	delete(r.handles, l)
	r.mu.Unlock()
	return nil
}

func clone(e backend.Entry) backend.Entry {
	e.Data = bytes.Clone(e.Data)
	return e
}
