package journal

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/craggy-io/artifact-services/provenance"
)

// subscriber coalesces: it always delivers the newest sequence, on its own
// goroutine, so a slow hook can never stall a commit. Missed intermediate values
// are recovered with Changes.
type subscriber struct {
	hook   func(uint64)
	latest atomic.Uint64
	wake   chan struct{}
	done   chan struct{}
	once   sync.Once
}

func (u *subscriber) stop() { u.once.Do(func() { close(u.done) }) }

func (u *subscriber) run() {
	delivered := uint64(0)
	for {
		select {
		case <-u.done:
			return
		case <-u.wake:
		}
		if sequence := u.latest.Load(); sequence > delivered {
			delivered = sequence
			u.hook(sequence)
		}
	}
}

func (s *Store) notify(sequence uint64) {
	for u := range s.subscribers {
		u.latest.Store(sequence)
		select {
		case u.wake <- struct{}{}:
		default:
		}
	}
}

// OnCommit registers a hook fired after each durable commit with the newest
// sequence. Calls are serial per hook and may skip sequences. The returned stop
// function does not wait for an in-flight call, so a hook may call it.
func (s *Store) OnCommit(hook func(uint64)) (stop func()) {
	u := &subscriber{hook: hook, wake: make(chan struct{}, 1), done: make(chan struct{})}
	s.mu.Lock()
	closed := s.closed.Load()
	if !closed {
		s.subscribers[u] = struct{}{}
	}
	s.mu.Unlock()
	if closed {
		return func() {}
	}
	go u.run()
	return func() {
		u.stop()
		s.mu.Lock()
		delete(s.subscribers, u)
		s.mu.Unlock()
	}
}

const (
	defaultChangeLimit = 100
	maxChangeLimit     = 1000
)

var errPageFull = errors.New("journal: page full")

// Changes returns committed records after q.After in sequence order. decode
// classifies each record. Last is the position to resume from: it advances over
// records filtered out, so a caller never rescans them.
func (s *Store) Changes(ctx context.Context, q provenance.ChangeQuery, decode func(Entry) (provenance.Change, error)) (provenance.ChangePage, error) {
	limit := q.Limit
	if limit == 0 {
		limit = defaultChangeLimit
	}
	if limit < 0 || limit > maxChangeLimit {
		return provenance.ChangePage{}, provenance.ErrChangeLimit
	}
	page := provenance.ChangePage{Last: q.After, Changes: []provenance.Change{}}
	err := s.Scan(ctx, q.After, 0, func(entry Entry) error {
		change, err := decode(entry)
		if err != nil {
			return err
		}
		change.Sequence = entry.Sequence
		if !change.Internal || q.IncludeInternal {
			page.Changes = append(page.Changes, change)
		}
		page.Last = entry.Sequence
		if len(page.Changes) == limit {
			return errPageFull
		}
		return nil
	})
	if err != nil && !errors.Is(err, errPageFull) {
		return provenance.ChangePage{}, err
	}
	return page, nil
}
