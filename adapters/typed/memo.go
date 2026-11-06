package typed

import (
	"context"
	"runtime"
	"sync"
	"weak"

	"github.com/craggy-io/artifact-services/assertions"
)

// Memo builds a value once per assertions service and key, for values that must
// be constructed against a live service (kinds from DefineKind, bundles of them)
// rather than at package level. Concurrent first calls for one service and key
// build once; the others wait and share the result. A build error is not cached,
// so the next call builds again. Entries go when the service is garbage collected.
//
// The key names the value: every caller using one key must build the same type T.
func Memo[T any](ctx context.Context, service *assertions.Service, key string, build func(context.Context) (T, error)) (T, error) {
	k := memoKey{service: weak.Make(service), key: key}
	v, loaded := memos.LoadOrStore(k, &memoEntry{})
	if !loaded {
		runtime.AddCleanup(service, func(k memoKey) { memos.Delete(k) }, k)
	}
	e := v.(*memoEntry)
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.done {
		return e.value.(T), nil
	}
	value, err := build(ctx)
	if err != nil {
		var zero T
		return zero, err
	}
	e.value, e.done = value, true
	return value, nil
}

type memoKey struct {
	service weak.Pointer[assertions.Service]
	key     string
}

type memoEntry struct {
	mu    sync.Mutex
	done  bool
	value any
}

var memos sync.Map // memoKey -> *memoEntry
