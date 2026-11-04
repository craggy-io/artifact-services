# Verification and measured evidence

[Project](../README.md) · [Documentation index](README.md) · [Walkthrough](walkthrough.md)

Requires Go 1.27 or newer, with no external dependencies.

```sh
test -z "$(gofmt -l .)"
go test ./integration -count=1
go test ./...
go test -race ./...
go vet ./...
go build ./...
(cd adapters/sqlindex && go test ./... && go vet ./...)
```

`adapters/sqlindex` is a separate module, so the root commands do not include it.

Tests also cover default internal event/stat exclusion, diagnostic opt-in, sticky
classification across concurrent real storage operations, context isolation,
retained aggregate selection, and visible parent failures/inclusive duration.

Tests cover storage roundtrips, immutable/concurrent writes, failure cleanup,
invalid keys, source/OS error preservation, publication outcomes, context
inheritance, sibling linkage, per-layer counters, cancellation, and reader
lifecycles. Windows tests exercise Unicode/spaces, actual long-path roundtrips
(321-character root / 453-character blob path), invalid root characters, and
ACL-denied writes/reads in disposable fixtures. ACL tests skip only if icacls is
unavailable. Cleanup failures are narrowly fault-injected; disk-full and filesystem
sync/close failures are not qualified by these tests.

## Crash qualification

`TestKillAtEveryWriteStageBoundary` runs a composite workload through a non-relaxed stack:
content, publication, occurrence, session, plain and deterministic runs, assertions with a
replacement and a retraction, guarded revisions. A clean run counts how often each boundary in
`crashpoint.All` is reached, and the test fails if the workload misses one. For **every hit of
every boundary** it then kills a real process at that point without cleanup, and asserts that:

- the store reopens without file surgery;
- everything the victim acknowledged is present;
- `gc.Verify` finds nothing, and each feed's sequences are contiguous;
- a session without an end record is reported unfinished;
- rerunning the identical workload reaches exactly the clean run's state.

`journal.torn_write` writes and syncs half a frame before dying. A separate check shows the
clean workload is itself idempotent. `TestLiveOwnerIsExcludedAndDeadOwnerIsNot` holds a stack
open in another process, requires `ErrLeaseHeld` naming that PID, kills it, and requires the
next open to succeed.

On 2026-11-04 this ran on Windows amd64 (NTFS) and on Linux amd64 (`golang:1.27` container,
non-root, overlay filesystem), 145 kills each, so the Unix lease and directory-sync code has
executed. On Linux, the full `go test ./...`, the nested module's tests, and `-race` for
`integration`, `internal/journal`, `storage` and `assertions` also passed. macOS has only been
vetted and built, not run.

This qualifies process death. It cannot show that a flush reached stable media, so power loss
is implemented as described in [durability](durability.md) but not physically tested.

## Opt-in 1 GiB Windows trial

```powershell
$env:ARTIFACT_SERVICES_LARGE_TEST = '1'
try { go test ./storage -run '^TestLargeBlob1GiB$' -count=1 -v }
finally { Remove-Item Env:ARTIFACT_SERVICES_LARGE_TEST }
```

The test checks at least 2 GiB free on the temporary volume, generates deterministic
bytes as a stream, verifies SHA-256 and exact byte counts, asserts publication,
reader completion, progress/metrics and temporary cleanup, then removes its owned
fixture. Routine tests skip this trial. No whole-blob buffer is allocated; the
trial fails if sampled `HeapAlloc` grows more than 256 MiB over its baseline.

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

The table above is the original storage baseline, before category filtering.
A fresh storage trial after filtering and extraction of the shared streaming test
helper passed: write 2.459 s (416.44 MiB/s), read 0.767 s (1,334.53 MiB/s),
HeapAlloc baseline/sample maximum 0.38/3.44 MiB, 322 samples, 120 events.

A separate CAS-over-storage trial also passed with the same exact byte count and
digest: write 3.621 s (282.81 MiB/s), read 1.253 s (817.24 MiB/s), HeapAlloc
baseline/sample maximum 0.38/3.60 MiB, 487 samples. It verifies the CAS identity,
EOF integrity, both layers' counters, and staging cleanup. Run it with the same
environment switch using `go test ./integration -run '^TestLargeCAS1GiB$' -count=1 -v`.
Both fresh trials ran on 2026-11-04 with Go 1.27.3 windows/amd64.

Both trials ran again on 2026-11-04 after durable publication added name flushing to Put, and
passed: storage write 2.991 s (342.41 MiB/s), read 0.802 s (1,276.29 MiB/s), HeapAlloc
baseline/sample maximum 0.38/3.45 MiB; CAS write 3.874 s (264.34 MiB/s), read 1.305 s
(784.97 MiB/s), HeapAlloc baseline/sample maximum 0.39/3.47 MiB. These are the current figures.

This is one local trial, not a storage benchmark guarantee. Read performance may
benefit from OS caching. HeapAlloc samples can miss transient peaks and exclude
process working set, native allocations and OS cache; they are not exact peak
memory measurements. Results do not qualify other mounts.

## Opt-in scale trials

Setting `ARTIFACT_SERVICES_LARGE_TRIALS=1` raises `TestBuilderPublishesLargeTreesWithBoundedHeap`
from 50,000 to 1,000,000 entries and enables `TestMillionRowTrial`. The tree trial fails if heap
measured after publishing and after paging has grown more than 64 MiB. The row trial samples heap
continuously and fails past 256 MiB. Both read `HeapAlloc`, with the limits noted above.

Measured on 2026-11-04, Go 1.27.3 windows/amd64: the tree trial published, paged and opened
1,000,000 entries in 76.0 s, and the row trial validated, iterated and queried 1,000,000 rows
in 112.2 s with a peak sampled heap of 52 MiB against a 256 MiB bound. One local run, not a
performance guarantee.

## Cross-layer implementation checks

On 2026-11-04, the full normal and race-enabled suites, `go vet ./...`, and
`go build ./...` passed on Windows amd64. Routine skips were the two opt-in large
trials (both executed separately) and packages containing no tests. The CAS,
artifact, and observability guide examples also ran successfully from a separate
Go module consuming the public API. Local documentation links resolve.

The integration tests use the real local backends to exercise APK ingestion,
runtime schema definition, typed table/JSON publication, schema evolution,
separate revision histories sharing content, exact saved-view inputs, SQL
join/filter/arithmetic/aggregation, materialization, and paginated row/cell/column
provenance. They reject forged typed metadata and invalid typed data, and check
internal artifact/event visibility. See [the walkthrough](walkthrough.md).

Focused package tests cover CAS corruption/reuse and commit stages; artifact
catalog replay/filtering/cursors; malformed schemas, exact-number arithmetic and
query limits; lineage evidence validation, traversal limits and history isolation;
and cross-process immutable storage publication. Shared-helper regressions cover
canonical-number roundtrips and expansion budgets, strict journal replay and
bounded config reads, and query-bound snapshot cursor isolation.

Each local journal requires one live owner per root. There is no distributed database,
global cross-layer transaction or arbitrary SQL engine in core, and multi-process read-only
access to a live root is not provided.
Detailed lineage describes supported emitted-result dependencies, not why-not or
counterfactual explanations. Large-file measurements qualify streaming CAS/storage;
typed queries intentionally use separate bounded workloads.
