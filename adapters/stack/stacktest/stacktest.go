// Package stacktest checks a whole stack over a given pair of backends: publication,
// guarded histories, assertions, change feeds and reopening. The local backends run
// it in this module; adapters/postgres and adapters/gcs run it against real servers,
// including two stacks serving one shared backend at once.
package stacktest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/craggy-io/artifact-services/adapters/stack"
	"github.com/craggy-io/artifact-services/artifacts"
	"github.com/craggy-io/artifact-services/lineage"
	"github.com/craggy-io/artifact-services/provenance"
)

// Fresh prepares empty backends for one test and returns a function that opens a
// stack over them. Each call opens another, independent stack over the same
// backends, as another process would; the caller closes it.
type Fresh func(t *testing.T) func() (*stack.Stack, error)

func open(t *testing.T, opener func() (*stack.Stack, error)) *stack.Stack {
	t.Helper()
	s, err := opener()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func publish(t *testing.T, s *stack.Stack, path, data string) artifacts.ID {
	t.Helper()
	ctx := context.Background()
	put, err := s.CAS.Put(ctx, strings.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	id, err := s.Artifacts.Publish(ctx, artifacts.Manifest{Version: 1, Entries: []artifacts.Entry{{Path: path, Content: put.ID, Size: put.Size}}})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func appendRevision(s *stack.Stack, history string, id, expected lineage.ID, artifact artifacts.ID) error {
	_, err := s.Lineage.Append(context.Background(), lineage.AppendRevision{ID: id, History: history, Expected: expected, Artifact: artifact})
	return err
}

func requireHead(t *testing.T, s *stack.Stack, history string, want lineage.ID) {
	t.Helper()
	head, err := s.Lineage.Head(context.Background(), history)
	if err != nil || head != want {
		t.Fatalf("head of %s = %q, %v; want %q", history, head, err, want)
	}
}

// Run checks one stack at a time. When shared is true, it also runs two stacks over
// the same backends at once and checks they behave as one.
func Run(t *testing.T, fresh Fresh, shared bool) {
	t.Run("PublishGuardAndReopen", func(t *testing.T) {
		ctx := context.Background()
		opener := fresh(t)
		s, err := opener()
		if err != nil {
			t.Fatal(err)
		}
		artifact := publish(t, s, "note.md", "# hello")
		if err := appendRevision(s, "h", "rev-1", "", artifact); err != nil {
			t.Fatal(err)
		}
		if err := appendRevision(s, "h", "rev-2", "rev-1", artifact); err != nil {
			t.Fatal(err)
		}
		if err := appendRevision(s, "h", "rev-3", "rev-1", artifact); !errors.Is(err, lineage.ErrHeadConflict) {
			t.Fatalf("stale head: %v", err)
		}
		// A retry of a committed revision reuses it even though the head moved on.
		if err := appendRevision(s, "h", "rev-2", "rev-1", artifact); err != nil {
			t.Fatalf("idempotent retry: %v", err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		again := open(t, opener)
		requireHead(t, again, "h", "rev-2")
		manifest, err := again.Artifacts.Get(ctx, artifact)
		if err != nil || len(manifest.Entries) != 1 || manifest.Entries[0].Path != "note.md" {
			t.Fatal(manifest, err)
		}
		reader, err := again.Artifacts.OpenFile(ctx, artifact, "note.md")
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(reader)
		if err = errors.Join(err, reader.Close()); err != nil || string(body) != "# hello" {
			t.Fatalf("content %q: %v", body, err)
		}
		page, err := again.Lineage.Changes(ctx, provenance.ChangeQuery{IncludeInternal: true})
		if err != nil || len(page.Changes) != 2 || page.Last != 2 {
			t.Fatal(page, err)
		}
	})
	if !shared {
		return
	}
	t.Run("SharedStacksReadEachOthersWrites", func(t *testing.T) {
		ctx := context.Background()
		opener := fresh(t)
		a, b := open(t, opener), open(t, opener)
		artifact := publish(t, a, "a.txt", "from a")
		if _, err := b.Artifacts.Get(ctx, artifact); err != nil {
			t.Fatalf("b cannot read a's artifact: %v", err)
		}
		if err := appendRevision(a, "h", "rev-a", "", artifact); err != nil {
			t.Fatal(err)
		}
		requireHead(t, b, "h", "rev-a")
		if err := appendRevision(b, "h", "rev-b", "rev-a", artifact); err != nil {
			t.Fatal(err)
		}
		requireHead(t, a, "h", "rev-b")
	})
	t.Run("SharedStacksHaveOneWinnerPerRound", func(t *testing.T) {
		opener := fresh(t)
		a, b := open(t, opener), open(t, opener)
		artifact := publish(t, a, "x.txt", "x")
		head := lineage.ID("")
		for round := 0; round < 10; round++ {
			ids := []lineage.ID{lineage.ID(fmt.Sprintf("a-%d", round)), lineage.ID(fmt.Sprintf("b-%d", round))}
			errs := make([]error, 2)
			var wait sync.WaitGroup
			for i, s := range []*stack.Stack{a, b} {
				wait.Add(1)
				go func(i int, s *stack.Stack) {
					defer wait.Done()
					errs[i] = appendRevision(s, "race", ids[i], head, artifact)
				}(i, s)
			}
			wait.Wait()
			var winner lineage.ID
			for i, err := range errs {
				switch {
				case err == nil:
					if winner != "" {
						t.Fatalf("round %d: two winners", round)
					}
					winner = ids[i]
				case errors.Is(err, lineage.ErrHeadConflict):
				default:
					t.Fatalf("round %d: %v", round, err)
				}
			}
			if winner == "" {
				t.Fatalf("round %d: no winner: %v", round, errs)
			}
			requireHead(t, a, "race", winner)
			requireHead(t, b, "race", winner)
			head = winner
		}
	})
}
