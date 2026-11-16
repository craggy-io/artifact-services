package pager

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestSnapshotReplayAndSelection(t *testing.T) {
	c := New[map[string]int]()
	build := func() ([]map[string]int, error) { return []map[string]int{{"n": 1}, {"n": 2}, {"n": 3}}, nil }
	p, next, err := c.Page("query", "", 1, build)
	if err != nil || p[0]["n"] != 1 || next == "" {
		t.Fatal(p, next, err)
	}
	p[0]["n"] = 99
	p, _, err = c.Page("query", next, 1, nil)
	if err != nil || p[0]["n"] != 2 {
		t.Fatal(p, err)
	}
	p[0]["n"] = 99
	p, _, err = c.Page("query", next, 1, nil)
	if err != nil || p[0]["n"] != 2 {
		t.Fatal("replay changed")
	}
	if _, _, err = c.Page("other", next, 1, nil); !errors.Is(err, ErrCursor) {
		t.Fatal(err)
	}
	c.now = func() time.Time { return time.Now().Add(time.Hour) }
	if _, _, err = c.Page("query", next, 1, nil); !errors.Is(err, ErrCursor) {
		t.Fatal(err)
	}
}

func TestAbandonedSnapshotsDoNotBlockFreshQueries(t *testing.T) {
	c := New[int]()
	clock := time.Now()
	c.now = func() time.Time { return clock }
	build := func() ([]int, error) { return []int{1, 2}, nil }
	var first, latest string
	for i := 0; i < 129; i++ {
		clock = clock.Add(time.Second)
		_, cursor, err := c.Page(fmt.Sprintf("query-%d", i), "", 1, build)
		if err != nil || cursor == "" {
			t.Fatalf("fresh query %d: cursor=%q err=%v", i, cursor, err)
		}
		if i == 0 {
			first = cursor
		}
		latest = cursor
	}
	if len(c.sessions) != 128 {
		t.Fatalf("retained %d sessions, want 128", len(c.sessions))
	}
	if _, _, err := c.Page("query-0", first, 1, nil); !errors.Is(err, ErrCursor) {
		t.Fatalf("displaced cursor: got %v, want ErrCursor", err)
	}
	page, _, err := c.Page("query-128", latest, 1, nil)
	if err != nil || len(page) != 1 || page[0] != 2 {
		t.Fatalf("latest cursor: page=%v err=%v", page, err)
	}
}

func TestSnapshotByteBudgetEvictsOldest(t *testing.T) {
	c := New[int]()
	now := time.Now()
	c.sessions["old"] = session[int]{expires: now, bytes: 40 << 20}
	c.sessions["new"] = session[int]{expires: now.Add(time.Minute), bytes: 20 << 20}
	c.bytes = 60 << 20
	c.makeRoom(8 << 20)
	if _, ok := c.sessions["old"]; ok || c.bytes != 20<<20 {
		t.Fatalf("byte eviction: sessions=%v bytes=%d", c.sessions, c.bytes)
	}
	if _, ok := c.sessions["new"]; !ok {
		t.Fatal("newest snapshot was evicted")
	}
}

func TestOversizedSnapshotStillFails(t *testing.T) {
	c := New[string]()
	_, _, err := c.Page("oversized", "", 1, func() ([]string, error) {
		return []string{strings.Repeat("x", 16<<20), "next"}, nil
	})
	if !errors.Is(err, ErrCapacity) {
		t.Fatalf("got %v, want ErrCapacity", err)
	}
}
