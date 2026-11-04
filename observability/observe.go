// Package observability provides optional context-scoped, layer-neutral events
// and process-local metrics. Layers explicitly report their own semantic work.
package observability

import (
	"context"
	"errors"
	"sync/atomic"
	"time"
)

// Scope separates metrics for each layer and operation. Use bounded labels.
type Scope struct{ Layer, Operation string }

// Category explicitly classifies work; it is never inferred from keys or names.
type Category uint8

const (
	// User is the default for direct operations, including direct storage calls.
	User Category = iota
	// Internal identifies supporting work excluded from ordinary reporting.
	Internal
)

func selected(category Category, includeInternal bool) bool {
	return category == User || includeInternal
}

// CategoryFrom returns this context's explicit inherited classification, even
// when observation is disabled. Ordinary contexts default to User. Layers use
// this value when persisting record classification; never infer it from keys.
// The context must be non-nil, as with the other context APIs in this package.
func CategoryFrom(ctx context.Context) Category {
	return bindingFrom(ctx).category
}

// WithInternal marks this context subtree as supporting work. Classification is
// sticky: With and Start preserve it; there is no descendant reset to User.
// Start user-facing siblings from the original user context instead.
func WithInternal(ctx context.Context) context.Context {
	b := bindingFrom(ctx)
	if b.category == Internal {
		return ctx
	}
	b.category = Internal
	return context.WithValue(ctx, contextKey{}, b)
}

// Event is a cumulative snapshot. Total=-1 means unknown. Counters are named
// semantic totals reported by the owning layer, not inferred from child spans.
// Final is emitted once. ParentID links to the scope active in the input context.
type Event struct {
	ID, ParentID uint64
	Scope        Scope
	Category     Category
	Key          string
	Bytes, Total int64
	Counters     map[string]int64
	Elapsed      time.Duration
	Final        bool
	Err          error
}

// Options are inherited through derived contexts. Callbacks are synchronous,
// may run concurrently across spans, must not panic, and may inspect collectors
// or start other spans, but must not reenter the same span/reader. Slow callbacks
// delay operations. No collector lock is held across callbacks.
type Options struct {
	// IncludeInternal enables future internal events, never replays past events.
	IncludeInternal bool
	Observer        func(Event)
	Collector       *Collector
	// Nonpositive intervals select 100ms. Start, semantic counts, errors and End
	// bypass throttling; only intermediate byte updates are throttled.
	ProgressInterval time.Duration
	// Total supplies an optional size hint by scope/key; negative/unknown is -1.
	Total func(Scope, string) (int64, bool)
}

type contextKey struct{}
type binding struct {
	options  Options
	parent   uint64
	category Category
}

func bindingFrom(ctx context.Context) binding {
	b, _ := ctx.Value(contextKey{}).(binding)
	return b
}

var nextID atomic.Uint64

// With attaches configuration to this context subtree, retaining its current
// parent link and category. Empty Options explicitly disable observation in the subtree.
// Contexts must be non-nil, as with standard context APIs.
func With(ctx context.Context, options Options) context.Context {
	b := bindingFrom(ctx)
	if options.ProgressInterval <= 0 {
		options.ProgressInterval = 100 * time.Millisecond
	}
	b.options = options
	return context.WithValue(ctx, contextKey{}, b)
}

// Span is a serially-used scope. Child spans may run concurrently. Methods on a
// nil span are no-ops. End is idempotent; reporting after End does nothing.
type Span struct {
	options     Options
	event       Event
	start, last time.Time
	ended       bool
}

// Start returns a derived context for child operations and a span. With no
// observer/collector, it returns the original context and nil without allocation.
func Start(ctx context.Context, scope Scope, key string) (context.Context, *Span) {
	b := bindingFrom(ctx)
	if b.options.Observer == nil && b.options.Collector == nil {
		return ctx, nil
	}
	now := time.Now()
	s := &Span{options: b.options, start: now, last: now, event: Event{ID: nextID.Add(1), ParentID: b.parent, Scope: scope, Category: b.category, Key: key, Total: -1}}
	if s.options.Total != nil {
		if n, known := s.options.Total(scope, key); known && n >= 0 {
			s.event.Total = n
		}
	}
	s.options.Collector.update(scope, b.category, func(m *Metrics) { m.Started++ })
	child := context.WithValue(ctx, contextKey{}, binding{options: b.options, parent: s.event.ID, category: b.category})
	s.emit()
	return child, s
}

func cloneCounters(src map[string]int64) map[string]int64 {
	if src == nil {
		return nil
	}
	dst := make(map[string]int64, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
func (s *Span) emit() {
	s.event.Elapsed = time.Since(s.start)
	if s.options.Observer != nil && selected(s.event.Category, s.options.IncludeInternal) {
		e := s.event
		e.Counters = cloneCounters(e.Counters)
		s.options.Observer(e)
	}
}

// AddBytes records stream bytes, including bytes delivered with an error.
func (s *Span) AddBytes(n int64) {
	if s == nil || s.ended || n <= 0 {
		return
	}
	s.event.Bytes += n
	s.options.Collector.update(s.event.Scope, s.event.Category, func(m *Metrics) { m.Bytes += n })
	if time.Since(s.last) >= s.options.ProgressInterval {
		s.last = time.Now()
		s.emit()
	}
}

// Count records a positive semantic delta (for example published, eof, or dedup).
// Each layer defines its own bounded counter names. Counters are never rolled up.
func (s *Span) Count(name string, n int64) {
	if s == nil || s.ended || n <= 0 {
		return
	}
	if s.event.Counters == nil {
		s.event.Counters = make(map[string]int64)
	}
	s.event.Counters[name] += n
	s.options.Collector.update(s.event.Scope, s.event.Category, func(m *Metrics) {
		if m.Counters == nil {
			m.Counters = make(map[string]int64)
		}
		m.Counters[name] += n
	})
	s.emit()
}

// RecordError reports a nonterminal error immediately, retaining it for End.
func (s *Span) RecordError(err error) {
	if s == nil || s.ended || err == nil {
		return
	}
	s.event.Err = errors.Join(s.event.Err, err)
	s.emit()
}

// End records the terminal outcome once. It does not end child spans, cancel
// contexts, or infer success/failure from cancellation; the owning layer decides.
func (s *Span) End(err error) {
	if s == nil || s.ended {
		return
	}
	s.ended = true
	s.event.Final = true
	s.event.Err = errors.Join(s.event.Err, err)
	s.event.Elapsed = time.Since(s.start)
	s.options.Collector.update(s.event.Scope, s.event.Category, func(m *Metrics) {
		m.Completed++
		m.Duration += s.event.Elapsed
		if s.event.Err != nil {
			m.Errors++
		}
	})
	s.emit()
}
