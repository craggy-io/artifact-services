// Package watch reports every commit to a stack's artifact, lineage and assertion
// journals exactly once, in sequence order, from a position the caller keeps. It
// composes the journals' public Sequence, Changes and OnCommit: it waits for commit
// notifications and reads from its position, so it never polls and a coalesced
// notification loses nothing.
package watch

import (
	"context"
	"sync"

	"github.com/craggy-io/artifact-services/adapters/stack"
	"github.com/craggy-io/artifact-services/provenance"
)

// Journal names, as reported on Batch.Journal.
const (
	Artifacts  = "artifacts"
	Lineage    = "lineage"
	Assertions = "assertions"
)

// Journal is the part of a journal-backed service the watch reads. The artifact,
// lineage and assertion services implement it.
type Journal interface {
	Sequence(ctx context.Context) (uint64, error)
	Changes(ctx context.Context, q provenance.ChangeQuery) (provenance.ChangePage, error)
	OnCommit(hook func(sequence uint64)) (stop func())
}

// Journals names the journals to watch. A nil journal is not watched.
type Journals struct {
	Artifacts  Journal
	Lineage    Journal
	Assertions Journal
}

// Of returns a stack's open journals.
func Of(s *stack.Stack) Journals {
	var j Journals
	if s.Artifacts != nil {
		j.Artifacts = s.Artifacts
	}
	if s.Lineage != nil {
		j.Lineage = s.Lineage
	}
	if s.Assertions != nil {
		j.Assertions = s.Assertions
	}
	return j
}

// Position is the last sequence reported from each journal. The zero Position is
// the start of every journal.
type Position struct {
	Artifacts  uint64 `json:"artifacts"`
	Lineage    uint64 `json:"lineage"`
	Assertions uint64 `json:"assertions"`
}

// Current returns each watched journal's latest sequence: watching from it reports
// only later commits.
func Current(ctx context.Context, j Journals) (Position, error) {
	var p Position
	for _, s := range j.sources() {
		sequence, err := s.journal.Sequence(ctx)
		if err != nil {
			return Position{}, err
		}
		*s.at(&p) = sequence
	}
	return p, nil
}

// Batch is consecutive changes from one journal, in sequence order, each with its
// record class. Position is where reading stands once the batch is handled: store it
// with the batch's effects, and resuming from it reports nothing twice.
type Batch struct {
	Journal  string
	Changes  []provenance.Change
	Position Position
}

// Handler handles one batch. An error leaves the batch unhandled and ends the read.
type Handler func(ctx context.Context, b Batch) error

// Options choose where reading starts and what it reports. Limit caps a batch's
// changes: zero selects 100, and the maximum is 1000. Internal records are reported
// only with IncludeInternal; records left out still advance the position.
type Options struct {
	From            Position
	IncludeInternal bool
	Limit           int
}

// Drain reports every change committed after o.From, up to the journals' tails when
// it was called, and returns the position reached. On error that position is the
// one after the last handled batch.
//
// Journals share no sequence, so order is per journal. Across journals a record is
// reported after the records it can reference: artifacts first, then lineage, then
// assertions, each bounded by a tail read before the journals it references.
func Drain(ctx context.Context, j Journals, o Options, handle Handler) (Position, error) {
	return drain(ctx, j, o, handle, func(Position) {})
}

func drain(ctx context.Context, j Journals, o Options, handle Handler, advance func(Position)) (Position, error) {
	position := o.From
	sources := j.sources()
	// Tails are read in reverse dependency order, so whatever a bounded record
	// references committed before its own journal is read.
	tails := make([]uint64, len(sources))
	for i := len(sources) - 1; i >= 0; i-- {
		tail, err := sources[i].journal.Sequence(ctx)
		if err != nil {
			return position, err
		}
		tails[i] = tail
	}
	for i, s := range sources {
		for *s.at(&position) < tails[i] {
			after := *s.at(&position)
			page, err := s.journal.Changes(ctx, provenance.ChangeQuery{After: after, Limit: o.Limit, IncludeInternal: o.IncludeInternal})
			if err != nil {
				return position, err
			}
			changes, last := bound(page, tails[i])
			if last <= after {
				break
			}
			next := position
			*s.at(&next) = last
			if len(changes) > 0 {
				if err := handle(ctx, Batch{Journal: s.name, Changes: changes, Position: next}); err != nil {
					return position, err
				}
			}
			position = next
			advance(position)
		}
	}
	return position, nil
}

// bound cuts a page at tail, the last sequence a drain reports.
func bound(page provenance.ChangePage, tail uint64) ([]provenance.Change, uint64) {
	if page.Last <= tail {
		return page.Changes, page.Last
	}
	n := 0
	for n < len(page.Changes) && page.Changes[n].Sequence <= tail {
		n++
	}
	return page.Changes[:n], tail
}

type source struct {
	name    string
	journal Journal
	at      func(*Position) *uint64
}

// sources lists the watched journals in dependency order.
func (j Journals) sources() []source {
	all := []source{
		{Artifacts, j.Artifacts, func(p *Position) *uint64 { return &p.Artifacts }},
		{Lineage, j.Lineage, func(p *Position) *uint64 { return &p.Lineage }},
		{Assertions, j.Assertions, func(p *Position) *uint64 { return &p.Assertions }},
	}
	out := all[:0]
	for _, s := range all {
		if s.journal != nil {
			out = append(out, s)
		}
	}
	return out
}

// Watch follows journals until stopped. Create one with Start.
type Watch struct {
	cancel   context.CancelFunc
	done     chan struct{}
	mu       sync.Mutex
	position Position
	err      error
}

// Start drains from o.From, then again after every commit, until Stop, the end of
// ctx, or a failed read or handle. Notifications coalesce and each drain reads from
// the watch's position, so a burst is one catch-up and nothing is skipped. Handle
// runs on the watch's goroutine, one batch at a time. Stop the watch before closing
// the journals.
func Start(ctx context.Context, j Journals, o Options, handle Handler) *Watch {
	ctx, cancel := context.WithCancel(ctx)
	w := &Watch{cancel: cancel, done: make(chan struct{}), position: o.From}
	wake := make(chan struct{}, 1)
	wake <- struct{}{} // the first drain catches up from o.From
	notify := func(uint64) {
		select {
		case wake <- struct{}{}:
		default: // a drain is already due, and it reads this commit too
		}
	}
	// Hooks are registered before the first drain, so no commit falls between.
	var stops []func()
	for _, s := range j.sources() {
		stops = append(stops, s.journal.OnCommit(notify))
	}
	go func() {
		defer close(w.done)
		defer func() {
			for _, stop := range stops {
				stop()
			}
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case <-wake:
			}
			from := o
			from.From = w.Position()
			if _, err := drain(ctx, j, from, handle, w.advance); err != nil {
				if ctx.Err() == nil {
					w.mu.Lock()
					w.err = err
					w.mu.Unlock()
				}
				return
			}
		}
	}()
	return w
}

func (w *Watch) advance(p Position) {
	w.mu.Lock()
	w.position = p
	w.mu.Unlock()
}

// Position returns the position after the last handled batch.
func (w *Watch) Position() Position {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.position
}

// Done is closed when the watch has ended.
func (w *Watch) Done() <-chan struct{} { return w.done }

// Stop ends the watch, waits for a batch in progress, and returns the read or
// handle error that ended it, or nil when it was stopped or its context ended. It
// may be called more than once, from any goroutine but the handler's.
func (w *Watch) Stop() error {
	w.cancel()
	<-w.done
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}
