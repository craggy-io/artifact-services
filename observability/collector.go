package observability

import (
	"sync"
	"time"
)

// Metrics describe explicitly observed work for one layer/operation. Active is
// Started-Completed. Duration sums completed lifetimes. Bytes include active work.
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
	totals map[Scope]Metrics
}

func (c *Collector) update(scope Scope, fn func(*Metrics)) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.totals == nil {
		c.totals = make(map[Scope]Metrics)
	}
	m := c.totals[scope]
	fn(&m)
	c.totals[scope] = m
}

// Snapshot returns an independent consistent copy, scoped to observed process
// operations. Layer metrics are not additive inventory or inclusive parent totals.
func (c *Collector) Snapshot() map[Scope]Metrics {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make(map[Scope]Metrics, len(c.totals))
	for k, m := range c.totals {
		m.Counters = cloneCounters(m.Counters)
		result[k] = m
	}
	return result
}
