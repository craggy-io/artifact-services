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
// It retains scope/counter labels, not keys or operation histories.
type Collector struct {
	mu     sync.Mutex
	totals map[bucket]Metrics
}

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
	m := c.totals[key]
	fn(&m)
	c.totals[key] = m
}

// Snapshot returns a selected independent copy of observed process operations.
// Internal aggregates are retained even when events were filtered; they can be
// included later. Nothing before this collector was attached can be recovered.
// IncludeInternal sums both buckets per scope; these are diagnostic work totals,
// not unique payload bytes or global inventory.
func (c *Collector) Snapshot(options SnapshotOptions) map[Scope]Metrics {
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
		combined.Bytes += m.Bytes
		combined.Duration += m.Duration
		if len(m.Counters) > 0 && combined.Counters == nil {
			combined.Counters = make(map[string]int64)
		}
		for name, value := range m.Counters {
			combined.Counters[name] += value
		}
		result[k.scope] = combined
	}
	return result
}
