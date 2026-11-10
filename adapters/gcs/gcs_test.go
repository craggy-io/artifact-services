package gcs_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/storage"

	"github.com/craggy-io/artifact-services/adapters/gcs"
	"github.com/craggy-io/artifact-services/adapters/stack"
	"github.com/craggy-io/artifact-services/adapters/stack/stacktest"
	"github.com/craggy-io/artifact-services/cas"
	blobs "github.com/craggy-io/artifact-services/storage"
)

// The tests need a GCS emulator (fake-gcs-server) at STORAGE_EMULATOR_HOST. They
// fail, never skip, without it.
var buckets atomic.Int64

func store(t *testing.T) *gcs.Store {
	t.Helper()
	if os.Getenv("STORAGE_EMULATOR_HOST") == "" {
		t.Fatal("STORAGE_EMULATOR_HOST is not set; these tests need a GCS emulator")
	}
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	name := fmt.Sprintf("test-%d-%d", os.Getpid(), buckets.Add(1))
	if err := client.Bucket(name).Create(ctx, "test-project", nil); err != nil {
		t.Fatal(err)
	}
	s, err := gcs.New(client, name, "blobs/")
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func read(t *testing.T, s *gcs.Store, key string) string {
	t.Helper()
	r, err := s.Open(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(r)
	if err = errors.Join(err, r.Close()); err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestPutOpenExistsStat(t *testing.T) {
	ctx := context.Background()
	s := store(t)
	if err := s.Put(ctx, "abcd", strings.NewReader("hello")); err != nil {
		t.Fatal(err)
	}
	if got := read(t, s, "abcd"); got != "hello" {
		t.Fatal(got)
	}
	if ok, err := s.Exists(ctx, "abcd"); !ok || err != nil {
		t.Fatal(ok, err)
	}
	if ok, err := s.Exists(ctx, "abce"); ok || err != nil {
		t.Fatal(ok, err)
	}
	if info, err := s.Stat(ctx, "abcd"); err != nil || info.Size != 5 {
		t.Fatal(info, err)
	}
	if _, err := s.Open(ctx, "abce"); !errors.Is(err, blobs.ErrNotFound) {
		t.Fatal(err)
	}
	if err := s.Put(ctx, "XYZ", strings.NewReader("x")); !errors.Is(err, blobs.ErrInvalidKey) {
		t.Fatal(err)
	}
}

func TestPutNeverReplaces(t *testing.T) {
	ctx := context.Background()
	s := store(t)
	if err := s.Put(ctx, "ab", strings.NewReader("first")); err != nil {
		t.Fatal(err)
	}
	err := s.Put(ctx, "ab", strings.NewReader("second"))
	var put *blobs.PutError
	if !errors.Is(err, blobs.ErrAlreadyExists) || !errors.As(err, &put) || put.Published {
		t.Fatal(err)
	}
	if got := read(t, s, "ab"); got != "first" {
		t.Fatal(got)
	}
}

// Storing content that is already stored reuses it: GCS's refusal of the second
// write is the store's "already exists", with nothing else wrong.
func TestCASReusesExistingContent(t *testing.T) {
	ctx := context.Background()
	c, err := cas.New(store(t), cas.Options{})
	if err != nil {
		t.Fatal(err)
	}
	first, err := c.Put(ctx, strings.NewReader("same content"))
	if err != nil || first.Reused {
		t.Fatal(first, err)
	}
	again, err := c.Put(ctx, strings.NewReader("same content"))
	if err != nil || !again.Reused || again.ID != first.ID {
		t.Fatal(again, err)
	}
}

type failing struct{ sent bool }

func (f *failing) Read(p []byte) (int, error) {
	if !f.sent {
		f.sent = true
		return copy(p, "partial"), nil
	}
	return 0, errors.New("source broke")
}

func TestFailedSourcePublishesNothing(t *testing.T) {
	ctx := context.Background()
	s := store(t)
	if err := s.Put(ctx, "cd", &failing{}); err == nil {
		t.Fatal("put succeeded")
	}
	if ok, err := s.Exists(ctx, "cd"); ok || err != nil {
		t.Fatal(ok, err)
	}
}

func TestConcurrentPutsHaveOneWinner(t *testing.T) {
	ctx := context.Background()
	s := store(t)
	var wins atomic.Int64
	var wait sync.WaitGroup
	for i := 0; i < 8; i++ {
		wait.Add(1)
		go func(i int) {
			defer wait.Done()
			err := s.Put(ctx, "ef", strings.NewReader(fmt.Sprint(i)))
			switch {
			case err == nil:
				wins.Add(1)
			case !errors.Is(err, blobs.ErrAlreadyExists):
				t.Error(err)
			}
		}(i)
	}
	wait.Wait()
	if wins.Load() != 1 {
		t.Fatal(wins.Load())
	}
}

func TestWalkAndRemove(t *testing.T) {
	ctx := context.Background()
	s := store(t)
	for _, key := range []string{"aa", "bb"} {
		if err := s.Put(ctx, key, strings.NewReader(key)); err != nil {
			t.Fatal(err)
		}
	}
	var keys []string
	if err := s.Walk(ctx, func(key string, _ blobs.Info) error { keys = append(keys, key); return nil }); err != nil {
		t.Fatal(err)
	}
	if strings.Join(keys, ",") != "aa,bb" {
		t.Fatal(keys)
	}
	if removed, err := s.Remove(ctx, "aa", time.Now().Add(-time.Hour)); removed || err != nil {
		t.Fatal("removed a recent blob", err)
	}
	if removed, err := s.Remove(ctx, "aa", time.Now().Add(time.Hour)); !removed || err != nil {
		t.Fatal(removed, err)
	}
	if ok, _ := s.Exists(ctx, "aa"); ok {
		t.Fatal("still present")
	}
	if n, err := s.SweepPending(ctx, time.Now()); n != 0 || err != nil {
		t.Fatal(n, err)
	}
}

func TestStackOverGCS(t *testing.T) {
	stacktest.Run(t, func(t *testing.T) func() (*stack.Stack, error) {
		blobStore, root := store(t), t.TempDir()
		return func() (*stack.Stack, error) {
			return stack.Open(stack.Options{Root: root, Blobs: blobStore, Relaxed: true})
		}
	}, false)
}
