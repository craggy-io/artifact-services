# artifact-services

A streaming Go library. Domain dependency direction: **artifacts → CAS → storage**.
All layers can report explicit work to the independent `observability` package
through a shared `context.Context`. Storage is available; CAS and artifacts have
no domain implementation yet. There is no server, CLI, or global telemetry state.

| Layer | Responsibilities | Public interfaces |
| --- | --- | --- |
| `storage` | Physical layout, streaming I/O, existence checks, atomic publication, and explicit storage observation/cancellation. | <ul><li><code>NewFilesystem(root string) (*Filesystem, error)</code></li><li><code>Put(ctx context.Context, key string, source io.Reader) error</code></li><li><code>Open(ctx context.Context, key string) (io.ReadCloser, error)</code></li><li><code>Exists(ctx context.Context, key string) (bool, error)</code></li></ul> |
| `observability` | Shared context configuration, parent-child scopes, throttled stream progress, semantic counters, and process-local aggregation by layer/operation. | <ul><li><code>With(ctx context.Context, options Options) context.Context</code></li><li><code>Start(ctx context.Context, scope Scope, key string) (context.Context, *Span)</code></li><li><code>(*Span).AddBytes(n int64)</code></li><li><code>(*Span).Count(name string, n int64)</code></li><li><code>(*Span).RecordError(err error)</code></li><li><code>(*Span).End(err error)</code></li><li><code>(*Collector).Snapshot() map[Scope]Metrics</code></li></ul> |
| CAS (`cas`) | Hashing and content identity over storage: store/read content by digest, verify identity, and deduplicate logical writes. | |
| `artifacts` | Organize artifacts over CAS: publish/read artifact manifests and metadata. | |

## Attach observation once

```go
package example

import (
    "context"
    "io"
    "strings"
    "time"

    "github.com/craggy-io/artifact-services/observability"
    "github.com/craggy-io/artifact-services/storage"
)

func RoundTrip(root string, output io.Writer) error {
    backend, err := storage.NewFilesystem(root)
    if err != nil { return err }
    var metrics observability.Collector
    ctx := observability.With(context.Background(), observability.Options{
        Collector: &metrics,
        ProgressInterval: 100 * time.Millisecond,
        Observer: func(event observability.Event) {
            // Send to a concurrency-safe logger/UI queue if desired.
            // event.Final ends a scope; bytes alone do not prove publication.
        },
    })
    if err := backend.Put(ctx, "abcd0123", strings.NewReader("hello")); err != nil {
        return err
    }
    reader, err := backend.Open(ctx, "abcd0123")
    if err != nil { return err }
    _, readErr := io.Copy(output, reader)
    closeErr := reader.Close()
    _ = metrics.Snapshot()
    if readErr != nil { return readErr }
    return closeErr
}
```

Each implemented layer starts its own scope and propagates the returned context.
For example, an application can group real storage calls without inventing CAS:

```go
ctx, operation := observability.Start(ctx,
    observability.Scope{Layer: "application", Operation: "upload"}, "request-id")
err := backend.Put(ctx, "abcd0123", source)
if err == nil { operation.Count("completed_uploads", 1) }
operation.End(err)
```

The storage span has the application span's ID as its ParentID. Derived contexts
(including cancellation/deadline contexts) inherit configuration and linkage.
Concurrent children started with the same parent context are siblings with unique
IDs. Parents do not automatically aggregate/end children or infer their semantic
outcomes. Future layers can report counters such as `dedup` explicitly; no CAS or
artifact functionality is supplied by observation itself.

## Observation contract

- `With` attaches options only to a context subtree; empty options disable
  observation there. Without observer/collector, `Start` returns the original
  context and a nil span without allocating a scope. All nil-span methods are
  no-ops. There is no global collector. Use non-nil contexts throughout.
- Observer and collector are independently optional. Collectors are zero-value
  usable and concurrency-safe; do not copy after use. Snapshots are independent
  copies keyed by `Scope{Layer, Operation}`, with started/completed/errors, bytes,
  summed completed duration, and named semantic counters. Active is started
  minus completed. Use bounded scope/counter labels; keys are not retained.
- Events carry ID/ParentID, scope/key, cumulative bytes/counters, elapsed time,
  optional total, error, and Final. `Options.Total` may supply a size hint by
  scope/key; absent, negative, or unknown values become -1. Totals are not
  validated or converted to a fabricated percentage.
- Byte updates are throttled per span (default 100ms); start, semantic counts,
  recorded errors, and final updates always emit. No timer emits during idle I/O.
  End is idempotent; reporting after End does nothing. Span methods require
  serial calls; independent child spans may run concurrently.
- Callbacks run synchronously and may run concurrently across spans. They must
  not panic or reenter the same span/reader. They may inspect snapshots or start
  other spans; no collector lock is held during callbacks. Slow callbacks delay
  work and contribute to lifetime duration.
- Storage reports scopes `storage/put`, `storage/open`, and `storage/exists`.
  Put increments `published` only after actual publication, even if cleanup
  fails. Open increments `eof` once, `closed` once, and `early_closed` when closed
  without observing EOF. Errors count terminal failing operations, not error
  values. Bytes count consumed/transferred stream bytes, including `(n, err)`.
- Open spans include opening and the returned reader's lifetime. EOF/read errors
  produce updates; only Close finalizes, preserving close errors. Final errors
  join read/close errors; Close returns only its original close error. Repeated
  Close returns the cached error without closing/counting again; reads after
  Close return `io.ErrClosedPipe`. Individual readers require serial use.
  Abandoned readers remain active. Reading exactly a known size without asking
  for EOF still counts as early close.
- Metrics cover only explicit operations observed in this process. Child bytes
  are not rolled into parents. Layer totals must not be summed as unique bytes,
  logical dedup savings, disk I/O/allocation, or persistent/global inventory.

## Storage and cancellation contract

`Store` is replaceable and safe for concurrent operations. It owns physical layout
and publication, not hashing, identity validation, deduplication, or metadata.

- Keys are 2–128 ASCII lowercase hexadecimal characters. Other characters,
  separators, uppercase, and traversal return `ErrInvalidKey`. Layout is one file
  per blob at `<root>/<first-two>/<key>`; paths stay internal except diagnostics.
- Put never overwrites. Concurrent same-key writers have one publisher; losing
  completed writers return `ErrAlreadyExists`. A duplicate may consume its source.
- Every Put failure is wrapped in `*storage.PutError`, whose Published field
  describes this call. Use `errors.As` to inspect it and `errors.Is`/`errors.As`
  for retained source/OS errors. Nil means published. Cleanup failure after
  publication has Published=true; pre-publication failure has Published=false,
  even if an input error contains another operation's published PutError.
- Open returns `ErrNotFound` for missing blobs; Exists returns false, nil. Other
  errors propagate; non-regular entries are errors. Callers close returned readers;
  storage never closes supplied source readers.
- Writes stream into a temporary file, sync and close it, then atomically create
  a hard link without replacement. Readers see absence or complete data.
  Temporary cleanup runs on success/failure; cleanup errors retain other causes.
  Empty shards can remain. An empty deleted root is recreated by a subsequent Put.
- All operations check cancellation before filesystem work. Put also checks
  around stream reads, before writes/sync, and immediately before publication.
  Open retains its context in the reader and checks between reads. Cancellation
  cannot interrupt an arbitrary blocked reader, filesystem call, or Sync. A read
  that returns bytes as cancellation arrives still counts those bytes.
- Cancellation observed before publication prevents publication and triggers
  cleanup. Cancellation racing the final check/hard-link operation may still
  publish. Once published, it is not rolled back: success (or Published=true on
  cleanup failure) remains authoritative even if the context is now canceled.
  Close always releases resources despite cancellation; cancellation alone after
  the last read does not manufacture a Close error.

The root must be private/trusted; external edits, symlinks, and junctions are
unsupported. No deletion/lifecycle API exists. A Docker-mounted root needs no
special code, but its filesystem must support atomic non-replacing hard links.
Unsupported filesystems return errors; there is no copy/overwrite fallback.
Directory entries are not synced, so crash/power-loss durability is not guaranteed.
Crashes can leave temporary files; automatic crash recovery is not implemented.

## Development and verification

Requires Go 1.27 or newer, with no external dependencies.

```sh
go fmt ./...
go test ./...
go test -race ./...
go vet ./...
go build ./...
```

Tests cover storage roundtrips, immutable/concurrent writes, failure cleanup,
invalid keys, source/OS error preservation, publication outcomes, context
inheritance, sibling linkage, per-layer counters, cancellation, and reader
lifecycles. Windows tests exercise Unicode/spaces, actual long-path roundtrips
(321-character root / 453-character blob path), invalid root characters, and
ACL-denied writes/reads in disposable fixtures. ACL tests skip only if icacls is
unavailable. Cleanup failures are narrowly fault-injected; disk-full, filesystem
sync/close failures, and crash recovery are not qualified by these tests.

### Opt-in 1 GiB Windows trial

```powershell
$env:ARTIFACT_SERVICES_LARGE_TEST = '1'
try { go test ./storage -run '^TestLargeBlob1GiB$' -count=1 -v }
finally { Remove-Item Env:ARTIFACT_SERVICES_LARGE_TEST }
```

The test checks at least 2 GiB free on the temporary volume, generates deterministic
bytes as a stream, verifies SHA-256 and exact byte counts, asserts publication,
reader completion, progress/metrics and temporary cleanup, then removes its owned
fixture. Routine tests skip this trial. No whole-blob buffer is allocated.

Measured on 2026-11-04, Go 1.27.3 windows/amd64, with context observation enabled:

| Measurement | Result |
| --- | --- |
| Verified bytes | 1,073,741,824 |
| Write (generation/hash/Sync included) | 2.539 s; 403.30 MiB/s |
| Read (hash included) | 0.745 s; 1,374.40 MiB/s |
| Go HeapAlloc baseline / sampled maximum | 0.38 / 3.45 MiB |
| Memory sampling | 328 samples at nominal 10 ms intervals |
| Observation events | 122 |
| SHA-256 | `9cc5601236c455c6af19a76e64d2d95953a93b10eeb8b8b756a57090e1499b3e` |

This is one local trial, not a storage benchmark guarantee. Read performance may
benefit from OS caching. HeapAlloc samples can miss transient peaks and exclude
process working set, native allocations and OS cache; they are not exact peak
memory measurements. Results do not qualify other mounts or crash durability.
