package pager

import (
	"errors"
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
