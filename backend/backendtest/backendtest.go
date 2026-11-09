// Package backendtest is the conformance suite every backend.Log must pass. The
// local file log runs it in this module; shared logs such as adapters/postgres run
// it against a real server.
package backendtest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/craggy-io/artifact-services/backend"
)

// Fresh returns an Opener for a new, empty root. Calling the Opener again opens
// another handle on that same root.
type Fresh func(t *testing.T) backend.Opener

var config = []byte(`{"suite":1}`)

func open(t *testing.T, opener backend.Opener) backend.Log {
	t.Helper()
	log, err := opener(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	return log
}

func noPrepare(context.Context, uint64) error { return nil }

func appendNew(t *testing.T, log backend.Log, key, data string) backend.Entry {
	t.Helper()
	e, existing, err := log.Append(context.Background(), key, []byte(data), noPrepare)
	if err != nil || existing {
		t.Fatalf("append %s: existing=%t err=%v", key, existing, err)
	}
	return e
}

// Run checks the single-writer semantics every log must have. When shared is true
// it also checks that two handles on one root serialise commits and wake watchers.
func Run(t *testing.T, fresh Fresh, shared bool) {
	t.Run("Empty", func(t *testing.T) {
		ctx := context.Background()
		log := open(t, fresh(t))
		if latest, err := log.Latest(ctx); err != nil || latest != 0 {
			t.Fatal(latest, err)
		}
		if _, ok, err := log.Get(ctx, "missing"); ok || err != nil {
			t.Fatal(ok, err)
		}
		if _, err := log.At(ctx, 1); !errors.Is(err, backend.ErrCorrupt) {
			t.Fatal(err)
		}
		if err := log.Scan(ctx, 0, 0, func(backend.Entry) error { return errors.New("visited") }); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("AppendGetAtScan", func(t *testing.T) {
		ctx := context.Background()
		log := open(t, fresh(t))
		for i := 1; i <= 25; i++ {
			e := appendNew(t, log, fmt.Sprintf("k%02d", i), fmt.Sprintf(`{"i":%d}`, i))
			if e.Sequence != uint64(i) {
				t.Fatalf("sequence %d, want %d", e.Sequence, i)
			}
		}
		if latest, err := log.Latest(ctx); err != nil || latest != 25 {
			t.Fatal(latest, err)
		}
		got, ok, err := log.Get(ctx, "k07")
		if err != nil || !ok || got.Sequence != 7 || string(got.Data) != `{"i":7}` {
			t.Fatal(got, ok, err)
		}
		at, err := log.At(ctx, 25)
		if err != nil || at.Key != "k25" || string(at.Data) != `{"i":25}` {
			t.Fatal(at, err)
		}
		var seen []uint64
		if err := log.Scan(ctx, 10, 20, func(e backend.Entry) error { seen = append(seen, e.Sequence); return nil }); err != nil {
			t.Fatal(err)
		}
		if len(seen) != 10 || seen[0] != 11 || seen[9] != 20 {
			t.Fatal(seen)
		}
		stop := errors.New("stop")
		if err := log.Scan(ctx, 0, 25, func(backend.Entry) error { return stop }); !errors.Is(err, stop) {
			t.Fatal(err)
		}
	})
	t.Run("ExistingKeySkipsPrepare", func(t *testing.T) {
		ctx := context.Background()
		log := open(t, fresh(t))
		appendNew(t, log, "a", `{"v":1}`)
		e, existing, err := log.Append(ctx, "a", []byte(`{"v":2}`), func(context.Context, uint64) error {
			t.Fatal("prepare ran for an existing key")
			return nil
		})
		if err != nil || !existing || e.Sequence != 1 || string(e.Data) != `{"v":1}` {
			t.Fatal(e, existing, err)
		}
	})
	t.Run("PrepareSeesLatestAndCanAbort", func(t *testing.T) {
		ctx := context.Background()
		log := open(t, fresh(t))
		appendNew(t, log, "a", `{}`)
		refused := errors.New("refused")
		var saw uint64
		_, _, err := log.Append(ctx, "b", []byte(`{}`), func(_ context.Context, latest uint64) error {
			saw = latest
			return refused
		})
		if !errors.Is(err, refused) || saw != 1 {
			t.Fatal(saw, err)
		}
		if _, ok, _ := log.Get(ctx, "b"); ok {
			t.Fatal("aborted record is present")
		}
		if e := appendNew(t, log, "b", `{}`); e.Sequence != 2 {
			t.Fatal(e)
		}
	})
	t.Run("PrepareCanReadTheLog", func(t *testing.T) {
		ctx := context.Background()
		log := open(t, fresh(t))
		appendNew(t, log, "a", `{"x":1}`)
		_, _, err := log.Append(ctx, "b", []byte(`{}`), func(ctx context.Context, latest uint64) error {
			var keys []string
			if err := log.Scan(ctx, 0, latest, func(e backend.Entry) error { keys = append(keys, e.Key); return nil }); err != nil {
				return err
			}
			if len(keys) != 1 || keys[0] != "a" {
				return fmt.Errorf("scan in prepare saw %v", keys)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("DataIsReturnedByteForByte", func(t *testing.T) {
		ctx := context.Background()
		log := open(t, fresh(t))
		data := []byte(`{"a":"é\n","b":[1,2.5,{"c":null}]}`)
		appendNew(t, log, "k", string(data))
		got, _, err := log.Get(ctx, "k")
		if err != nil || !bytes.Equal(got.Data, data) {
			t.Fatalf("%s %v", got.Data, err)
		}
	})
	t.Run("ConfigIsBound", func(t *testing.T) {
		ctx := context.Background()
		opener := fresh(t)
		log, err := opener(ctx, config)
		if err != nil {
			t.Fatal(err)
		}
		appendNew(t, log, "a", `{}`)
		if err := log.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := opener(ctx, []byte(`{"suite":2}`)); !errors.Is(err, backend.ErrConfig) {
			t.Fatalf("different config: %v", err)
		}
		again := open(t, opener)
		if latest, err := again.Latest(ctx); err != nil || latest != 1 {
			t.Fatal(latest, err)
		}
	})
	t.Run("ConcurrentAppendsAreContiguous", func(t *testing.T) {
		ctx := context.Background()
		log := open(t, fresh(t))
		var wait sync.WaitGroup
		for i := 0; i < 16; i++ {
			wait.Add(1)
			go func(i int) {
				defer wait.Done()
				if _, _, err := log.Append(ctx, fmt.Sprintf("w%d", i), []byte(`{}`), noPrepare); err != nil {
					t.Error(err)
				}
			}(i)
		}
		wait.Wait()
		requireContiguous(t, log, 16)
	})
	t.Run("ClosedLogRefuses", func(t *testing.T) {
		ctx := context.Background()
		log, err := fresh(t)(ctx, config)
		if err != nil {
			t.Fatal(err)
		}
		if err := log.Close(); err != nil {
			t.Fatal(err)
		}
		if _, _, err := log.Append(ctx, "a", []byte(`{}`), noPrepare); !errors.Is(err, backend.ErrClosed) {
			t.Fatal(err)
		}
	})
	if !shared {
		return
	}
	t.Run("SharedRootSerialisesWriters", func(t *testing.T) {
		ctx := context.Background()
		opener := fresh(t)
		a, b := open(t, opener), open(t, opener)
		woke := make(chan struct{}, 1)
		stop := b.Watch(func() {
			select {
			case woke <- struct{}{}:
			default:
			}
		})
		defer stop()
		var wait sync.WaitGroup
		for i := 0; i < 20; i++ {
			wait.Add(1)
			go func(i int) {
				defer wait.Done()
				log := a
				if i%2 == 1 {
					log = b
				}
				if _, _, err := log.Append(ctx, fmt.Sprintf("w%d", i), []byte(`{}`), noPrepare); err != nil {
					t.Error(err)
				}
			}(i)
		}
		wait.Wait()
		requireContiguous(t, a, 20)
		requireContiguous(t, b, 20)
		select {
		case <-woke:
		case <-time.After(10 * time.Second):
			t.Fatal("watch never fired for the other handle's commits")
		}
	})
	t.Run("SharedRootPrepareSeesOtherWriter", func(t *testing.T) {
		ctx := context.Background()
		opener := fresh(t)
		a, b := open(t, opener), open(t, opener)
		appendNew(t, a, "first", `{}`)
		var saw uint64
		if _, _, err := b.Append(ctx, "second", []byte(`{}`), func(_ context.Context, latest uint64) error {
			saw = latest
			return nil
		}); err != nil || saw != 1 {
			t.Fatal(saw, err)
		}
	})
}

func requireContiguous(t *testing.T, log backend.Log, want uint64) {
	t.Helper()
	ctx := context.Background()
	latest, err := log.Latest(ctx)
	if err != nil || latest != want {
		t.Fatalf("latest %d, want %d: %v", latest, want, err)
	}
	next := uint64(1)
	keys := map[string]bool{}
	if err := log.Scan(ctx, 0, latest, func(e backend.Entry) error {
		if e.Sequence != next || keys[e.Key] {
			return fmt.Errorf("entry %d %q at position %d", e.Sequence, e.Key, next)
		}
		keys[e.Key] = true
		next++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
