package typed_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/craggy-io/artifact-services/adapters/testkit"
	"github.com/craggy-io/artifact-services/adapters/typed"
)

// One definition per store per kind; concurrent first use defines once.
func TestMemoDefinesOncePerStore(t *testing.T) {
	a, b := testkit.Open(t), testkit.Open(t)
	var builds atomic.Int32
	define := func(s *testkit.Stack) func(ctx context.Context) (typed.Kind[int], error) {
		return func(ctx context.Context) (typed.Kind[int], error) {
			builds.Add(1)
			return typed.DefineKind[int](ctx, s.Assertions, typed.KindDefinition{Name: "memo", Version: 1})
		}
	}
	var wg sync.WaitGroup
	kinds := make([]typed.Kind[int], 16)
	for i := range kinds {
		wg.Go(func() {
			k, err := typed.Memo(a.Ctx, a.Assertions, "memo", define(a))
			if err != nil {
				t.Error(err)
			}
			kinds[i] = k
		})
	}
	wg.Wait()
	if builds.Load() != 1 {
		t.Fatalf("builds = %d, want 1", builds.Load())
	}
	for _, k := range kinds {
		if k.ID() != kinds[0].ID() {
			t.Fatal("concurrent callers got different kinds")
		}
	}
	if _, err := typed.Memo(b.Ctx, b.Assertions, "memo", define(b)); err != nil || builds.Load() != 2 {
		t.Fatalf("second store: builds = %d, err = %v; want a definition of its own", builds.Load(), err)
	}
}

func TestMemoDoesNotCacheErrors(t *testing.T) {
	s := testkit.Open(t)
	fail := errors.New("boom")
	if _, err := typed.Memo(s.Ctx, s.Assertions, "flaky", func(context.Context) (int, error) { return 0, fail }); !errors.Is(err, fail) {
		t.Fatalf("err = %v", err)
	}
	if v, err := typed.Memo(s.Ctx, s.Assertions, "flaky", func(context.Context) (int, error) { return 7, nil }); err != nil || v != 7 {
		t.Fatalf("retry = %d, %v", v, err)
	}
}
