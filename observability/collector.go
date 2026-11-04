package observability

import (
	"sync"
	"time"
)

// Metrics describe explicitly observed work for one layer/operation. Active is
// Started-Completed. Duration sums inclusive completed lifetimes (including
// hidden child work). Bytes include only explicit work reported on these spans.
type Metrics struct {
	Started, Completed, Errors uint64
	Bytes                      int64
	Duration                   time.Duration
	Counters                   map[string]int64
}

// Collector is concurrency-safe and zero-value usable. Do not copy after use.
// It retains scope/counter labels, not keys or operation histories. Retained
// labels are bounded (maxBuckets scopes, maxDistinctCounters counters each);
// work under labels beyond those bounds is dropped, so callers must keep
// scope/counter names bounded. Reset clears retained totals.
type Collector struct {
	mu     sync.Mutex
	totals map[bucket]Metrics
}

const maxBuckets = 1024

type bucket struct {
	scope    Scope
	category Category
}

// SnapshotOptions selects the reporting view, not what the collector retains.
// The default excludes internal work; IncludeInternal includes both categories.
type SnapshotOptions struct{ IncludeInternal bool }

func (c *Collector) update(scope Scope, category Category, fn func(*Metrics)) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.totals == nil {
		c.totals = make(map[bucket]Metrics)
	}
	key := bucket{scope: scope, category: category}
	m, ok := c.totals[key]
	if !ok && len(c.totals) >= maxBuckets {
		return
	}
	fn(&m)
	c.totals[key] = m
}

// Reset clears all retained totals. In-flight spans continue reporting into
// fresh buckets.
func (c *Collector) Reset() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.totals = nil
}

// Snapshot returns a selected independent copy of observed process operations.
// Internal aggregates are retained even when events were filtered; they can be
// included later. Nothing before this collector was attached can be recovered.
// IncludeInternal sums both buckets per scope; these are diagnostic work totals,
// not unique payload bytes or global inventory.
func (c *Collector) Snapshot(options SnapshotOptions) map[Scope]Metrics {
	if c == nil {
		return make(map[Scope]Metrics)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make(map[Scope]Metrics)
	for k, m := range c.totals {
		if !selected(k.category, options.IncludeInternal) {
			continue
		}
		combined := result[k.scope]
		combined.Started += m.Started
		combined.Completed += m.Completed
		combined.Errors += m.Errors
		combined.Bytes = satAdd(combined.Bytes, m.Bytes)
		combined.Duration = time.Duration(satAdd(int64(combined.Duration), int64(m.Duration)))
		if len(m.Counters) > 0 && combined.Counters == nil {
			combined.Counters = make(map[string]int64)
		}
		for name, value := range m.Counters {
			combined.Counters[name] = satAdd(combined.Counters[name], value)
		}
		result[k.scope] = combined
	}
	return result
}
