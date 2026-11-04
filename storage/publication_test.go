package storage

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"strings"
	"testing"
)

func TestPublicationCleanupOutcome(t *testing.T) {
	for _, publish := range []bool{true, false} {
		t.Run(map[bool]string{true: "published", false: "not published"}[publish], func(t *testing.T) {
			s, root := newTestStore(t)
			cleanup := &fs.PathError{Op: "remove", Path: "temporary", Err: fs.ErrPermission}
			sourceFailure := errors.New("source interrupted")
			var source interface{ Read([]byte) (int, error) } = strings.NewReader("complete")
			if !publish {
				source = failedReader{sourceFailure}
			}
			var temporary string
			err := s.put(context.Background(), "ab", source, func(path string) error { temporary = path; return cleanup })
			var outcome *PutError
			if !errors.As(err, &outcome) || outcome.Published != publish {
				t.Fatalf("publication status: %v", err)
			}
			if !errors.Is(err, fs.ErrPermission) {
				t.Fatalf("cleanup cause lost: %v", err)
			}
			var pathErr *fs.PathError
			if !errors.As(err, &pathErr) || pathErr != cleanup {
				t.Fatalf("cleanup OS error lost: %v", err)
			}
			if publish {
				if got := string(readBlob(t, s, "ab")); got != "complete" {
					t.Fatal(got)
				}
			} else {
				if !errors.Is(err, sourceFailure) {
					t.Fatalf("source cause lost: %v", err)
				}
				if exists, e := s.Exists(context.Background(), "ab"); exists || e != nil {
					t.Fatalf("published unexpectedly: %v %v", exists, e)
				}
			}
			if err := os.Remove(temporary); err != nil {
				t.Fatal(err)
			}
			assertNoPending(t, root)
		})
	}
}
func TestNestedPutErrorDoesNotClaimPublication(t *testing.T) {
	s, root := newTestStore(t)
	upstream := &PutError{Published: true, Err: fs.ErrPermission}
	err := s.Put(context.Background(), "ab", failedReader{upstream})
	var outcome *PutError
	if !errors.As(err, &outcome) || outcome.Published {
		t.Fatalf("wrong operation outcome: %v", err)
	}
	if !errors.Is(err, upstream) || !errors.Is(err, fs.ErrPermission) {
		t.Fatal("upstream cause lost")
	}
	if exists, e := s.Exists(context.Background(), "ab"); exists || e != nil {
		t.Fatalf("partial published: %v %v", exists, e)
	}
	assertNoPending(t, root)
}
