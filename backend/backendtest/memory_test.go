package backendtest_test

import (
	"context"
	"testing"

	"github.com/craggy-io/artifact-services/backend"
	"github.com/craggy-io/artifact-services/backend/backendtest"
)

func TestMemoryConformance(t *testing.T) {
	backendtest.Run(t, func(*testing.T) backend.Opener { return backendtest.NewMemory().Open("root") }, true)
}

func TestMemoryCopyIsAPointInTime(t *testing.T) {
	ctx := context.Background()
	m := backendtest.NewMemory()
	log, err := m.Open("root")(ctx, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	add := func(l backend.Log, key string) {
		if _, _, err := l.Append(ctx, key, []byte(`"x"`), func(context.Context, uint64) error { return nil }); err != nil {
			t.Fatal(err)
		}
	}
	add(log, "a")
	snap := m.Copy()
	add(log, "b")
	restored, err := snap.Open("root")(ctx, []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if latest, _ := restored.Latest(ctx); latest != 1 {
		t.Fatal(latest)
	}
	add(restored, "c")
	if e, ok, _ := restored.Get(ctx, "c"); !ok || e.Sequence != 2 {
		t.Fatal(e, ok)
	}
	if latest, _ := log.Latest(ctx); latest != 2 {
		t.Fatal(latest)
	}
}
