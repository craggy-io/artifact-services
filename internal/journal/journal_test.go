package journal

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/craggy-io/artifact-services/internal/canon"
	"github.com/craggy-io/artifact-services/provenance"
)

func mustOpen(t *testing.T, root string, o Options) *Store {
	t.Helper()
	s, err := Open(root, o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := s.CatchUp(context.Background()); err != nil {
		t.Fatal(err)
	}
	return s
}

// addRawFrame appends a crafted frame after the log's current contents.
func addRawFrame(t *testing.T, root string, payload []byte) {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(root, logName), os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err = f.Write(encodeFrame(payload)); err != nil {
		t.Fatal(err)
	}
}

func TestDurableConflictAndLease(t *testing.T) {
	root := t.TempDir()
	s, err := Open(root, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(root, Options{}); !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("duplicate lease = %v", err)
	}
	e, reused, err := s.Append(context.Background(), "id", []byte(`{"x":1.0}`))
	if err != nil || reused || e.Sequence != 1 {
		t.Fatal(e, reused, err)
	}
	_, reused, err = s.Append(context.Background(), "id", []byte(`{"x":1}`))
	if err != nil || !reused {
		t.Fatal(err)
	}
	if _, _, err = s.Append(context.Background(), "id", []byte(`{"x":2}`)); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s = mustOpen(t, root, Options{})
	e, ok, err := s.Get("id")
	if err != nil || !ok || string(e.Data) != `{"x":1}` {
		t.Fatal(e, ok, err)
	}
	e.Data[0] = '!'
	e, _, _ = s.Get("id")
	if e.Data[0] != '{' {
		t.Fatal("aliased data")
	}
}

func TestCancellationCapacityAndCorruption(t *testing.T) {
	root := t.TempDir()
	s, err := Open(root, Options{MaxRecords: 1})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err = s.Append(ctx, "x", []byte(`{}`)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, _, err = s.Append(context.Background(), "x", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if _, _, err = s.Append(context.Background(), "y", []byte(`{}`)); !errors.Is(err, ErrCapacity) {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	// A lowered limit stops new commits but must never stop a store opening.
	s = mustOpen(t, root, Options{MaxRecords: 1})
	s.Close()
	valid, _ := canon.Marshal(Entry{Sequence: 2, Key: "y", Data: []byte(`{}`)})
	bad := encodeFrame(valid)
	binary.LittleEndian.PutUint32(bad[4:8], 0)
	f, err := os.OpenFile(filepath.Join(root, logName), os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.Write(bad); err != nil {
		t.Fatal(err)
	}
	f.Close()
	// Bad checksum with a valid frame after it is corruption, not a torn tail.
	following, _ := canon.Marshal(Entry{Sequence: 3, Key: "z", Data: []byte(`{}`)})
	addRawFrame(t, root, following)
	if _, err = Open(root, Options{}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corruption accepted: %v", err)
	}
}

func TestReplayRejectsInvalidCanonicalRecords(t *testing.T) {
	cases := map[string][]byte{"numeric": []byte(`{"data":{"n":1.0},"key":"id","sequence":1}`), "duplicate": []byte(`{"data":{},"key":"id","key":"other","sequence":1}`), "unknown": []byte(`{"data":{},"extra":true,"key":"id","sequence":1}`)}
	longKey := strings.Repeat("x", 1025)
	cases["key"], _ = canon.Marshal(Entry{Sequence: 1, Key: longKey, Data: []byte(`{}`)})
	cases["sequence"], _ = canon.Marshal(Entry{Sequence: 9, Key: "id", Data: []byte(`{}`)})
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			s, err := Open(root, Options{})
			if err != nil {
				t.Fatal(err)
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			addRawFrame(t, root, data)
			if _, err = Open(root, Options{}); err == nil {
				t.Fatal("invalid persisted record accepted")
			}
		})
	}
}

func TestOpenRejectsOversizedConfigUnknownFilesAndFormats(t *testing.T) {
	for _, name := range []string{"config", "directory", "old-record", "format"} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			switch name {
			case "config":
				f, err := os.Create(filepath.Join(root, configName))
				if err != nil {
					t.Fatal(err)
				}
				if err = f.Truncate(9 << 20); err != nil {
					t.Fatal(err)
				}
				f.Close()
			case "directory":
				if err := os.Mkdir(filepath.Join(root, "bad"), 0700); err != nil {
					t.Fatal(err)
				}
			case "old-record":
				if err := os.WriteFile(filepath.Join(root, "00000000000000000001.json"), []byte(`{}`), 0600); err != nil {
					t.Fatal(err)
				}
			case "format":
				if err := os.WriteFile(filepath.Join(root, logName), []byte("ASJOURNAL9\nfuture"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			_, err := Open(root, Options{})
			if err == nil {
				t.Fatal("unexpected input accepted")
			}
			if name != "config" && !errors.Is(err, ErrFormat) {
				t.Fatalf("got %v, want ErrFormat", err)
			}
		})
	}
}

func TestTornTailIsTruncatedAndCommitsSurvive(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s := mustOpen(t, root, Options{})
	if _, _, err := s.Append(ctx, "a", []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	s.Close()
	info, err := os.Stat(filepath.Join(root, logName))
	if err != nil {
		t.Fatal(err)
	}
	torn, _ := canon.Marshal(Entry{Sequence: 2, Key: "b", Data: []byte(`{"n":2}`)})
	frame := encodeFrame(torn)
	for _, cut := range []int{1, frameHeader - 1, frameHeader, len(frame) - 1} {
		f, err := os.OpenFile(filepath.Join(root, logName), os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = f.Write(frame[:cut]); err != nil {
			t.Fatal(err)
		}
		f.Close()
		s := mustOpen(t, root, Options{})
		if s.Sequence() != 1 {
			t.Fatalf("cut %d: sequence %d", cut, s.Sequence())
		}
		if _, ok, _ := s.Get("b"); ok {
			t.Fatalf("cut %d: torn record visible", cut)
		}
		s.Close()
		after, _ := os.Stat(filepath.Join(root, logName))
		if after.Size() != info.Size() {
			t.Fatalf("cut %d: not truncated: %d != %d", cut, after.Size(), info.Size())
		}
	}
	s = mustOpen(t, root, Options{})
	e, _, err := s.Append(ctx, "b", []byte(`{"n":2}`))
	if err != nil || e.Sequence != 2 {
		t.Fatal(e, err)
	}
}

func TestLeaseIsReusableAndOwnerIsReported(t *testing.T) {
	root := t.TempDir()
	s := mustOpen(t, root, Options{})
	_, err := Open(root, Options{})
	var held *LeaseError
	if !errors.As(err, &held) || held.Owner.PID != os.Getpid() {
		t.Fatalf("got %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// The file stays: its presence means nothing, only the OS lock does.
	if _, err := os.Stat(filepath.Join(root, leaseName)); err != nil {
		t.Fatal(err)
	}
	mustOpen(t, root, Options{})
}

func TestCommitHooksRunAtomicallyWithTheRecord(t *testing.T) {
	ctx := context.Background()
	s := mustOpen(t, t.TempDir(), Options{Relaxed: true})
	refused := errors.New("refused")
	if _, _, err := s.Commit(ctx, Txn{Key: "a", Data: []byte(`{}`), Check: func() error { return refused }}); !errors.Is(err, refused) {
		t.Fatal(err)
	}
	if s.Sequence() != 0 {
		t.Fatal("check failure committed")
	}
	// A guard under the commit lock admits one winner however writers interleave.
	var winners atomic.Int64
	var applied []uint64
	var taken bool
	var wait sync.WaitGroup
	for i := 0; i < 32; i++ {
		wait.Add(1)
		go func(i int) {
			defer wait.Done()
			_, _, err := s.Commit(ctx, Txn{
				Key:  "writer-" + string(rune('A'+i)),
				Data: []byte(`{}`),
				Check: func() error {
					if taken {
						return refused
					}
					return nil
				},
				Apply: func(e Entry) { taken = true; applied = append(applied, e.Sequence) },
			})
			if err == nil {
				winners.Add(1)
			}
		}(i)
	}
	wait.Wait()
	if winners.Load() != 1 || len(applied) != 1 || applied[0] != s.Sequence() {
		t.Fatal(winners.Load(), applied, s.Sequence())
	}
	// A replay of a committed record reuses without running the guard.
	if _, reused, err := s.Commit(ctx, Txn{Key: "a", Data: []byte(`{}`), Check: func() error { return nil }}); err != nil || reused {
		t.Fatal(reused, err)
	}
	if _, reused, err := s.Commit(ctx, Txn{Key: "a", Data: []byte(`{}`), Check: func() error { return refused }}); err != nil || !reused {
		t.Fatal(reused, err)
	}
}

func TestScanAtAndChanges(t *testing.T) {
	ctx := context.Background()
	s := mustOpen(t, t.TempDir(), Options{Relaxed: true})
	for i := 0; i < 50; i++ {
		if _, _, err := s.Append(ctx, string(rune('A'+i)), []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
	}
	var seen []uint64
	if err := s.Scan(ctx, 10, 20, func(e Entry) error { seen = append(seen, e.Sequence); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 10 || seen[0] != 11 || seen[9] != 20 {
		t.Fatal(seen)
	}
	if e, err := s.At(50); err != nil || e.Sequence != 50 {
		t.Fatal(e, err)
	}
	if _, err := s.At(51); err == nil {
		t.Fatal("missing sequence")
	}
	decode := func(e Entry) (provenance.Change, error) {
		return provenance.Change{Class: "x", ID: e.Key, Internal: e.Sequence%2 == 0}, nil
	}
	page, err := s.Changes(ctx, provenance.ChangeQuery{After: 0, Limit: 5}, decode)
	if err != nil || len(page.Changes) != 5 || page.Changes[4].Sequence != 9 || page.Last != 9 {
		t.Fatal(page, err)
	}
	page, err = s.Changes(ctx, provenance.ChangeQuery{After: 9, Limit: 5, IncludeInternal: true}, decode)
	if err != nil || len(page.Changes) != 5 || page.Changes[0].Sequence != 10 {
		t.Fatal(page, err)
	}
	if _, err = s.Changes(ctx, provenance.ChangeQuery{Limit: 1001}, decode); !errors.Is(err, provenance.ErrChangeLimit) {
		t.Fatal(err)
	}
}

func TestOnCommitNeverBlocksCommitsAndDeliversNewest(t *testing.T) {
	ctx := context.Background()
	s := mustOpen(t, t.TempDir(), Options{Relaxed: true})
	release := make(chan struct{})
	var latest atomic.Uint64
	stop := s.OnCommit(func(sequence uint64) {
		<-release
		latest.Store(sequence)
	})
	defer stop()
	for i := 0; i < 20; i++ {
		if _, _, err := s.Append(ctx, string(rune('A'+i)), []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
	}
	close(release)
	deadline := time.Now().Add(5 * time.Second)
	for latest.Load() != 20 {
		if time.Now().After(deadline) {
			t.Fatalf("latest %d", latest.Load())
		}
		time.Sleep(time.Millisecond)
	}
}
