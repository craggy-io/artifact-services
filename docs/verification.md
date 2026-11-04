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
`integration`, `internal/journal`, `storage` and `assertions` also passed. macOS had only
been vetted and built at that point; since the CI workflow landed, the full suite (including
this crash qualification and the Darwin lease/directory-sync paths) passes on `macos-latest`
for both v0.1.0-tagged commits (`e8736ec`, `2cc9133`).

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

The CAS trial runs with the same environment switch:
`go test ./integration -run '^TestLargeCAS1GiB$' -count=1 -v`. It verifies the CAS
identity, EOF integrity, both layers' counters, and staging cleanup.

Current figures, re-measured on 2026-11-04, Go 1.27.3 windows/amd64, with context
observation and durable (non-relaxed) publication enabled. Both trials verified
1,073,741,824 bytes with SHA-256
`9cc5601236c455c6af19a76e64d2d95953a93b10eeb8b8b756a57090e1499b3e`:

| Trial | Write | Read | HeapAlloc baseline / sampled peak |
| --- | --- | --- | --- |
| Storage (`TestLargeBlob1GiB`) | 2.812 s; 364.19 MiB/s | 0.802 s; 1,277.25 MiB/s | 0.38 / 3.45 MiB (361 samples) |
| CAS over storage (`TestLargeCAS1GiB`) | 3.940 s; 259.88 MiB/s | 1.274 s; 803.81 MiB/s | 0.39 / 3.60 MiB (521 samples) |

Write/read include byte generation, hashing and observation. Earlier figures in
git history predate durable publication and are superseded by this table.

This is one local trial, not a storage benchmark guarantee. Read performance may
benefit from OS caching. HeapAlloc samples can miss transient peaks and exclude
process working set, native allocations and OS cache; they are not exact peak
memory measurements. Results do not qualify other mounts.

## Opt-in scale trials

Setting `ARTIFACT_SERVICES_LARGE_TRIALS=1` raises `TestBuilderPublishesLargeTreesWithBoundedHeap`
from 50,000 to 1,000,000 entries and enables `TestMillionRowTrial`. The tree trial fails if heap
measured after publishing and after paging has grown more than 64 MiB. The row trial samples heap
continuously and fails past 256 MiB. Both read `HeapAlloc`, with the limits noted above.

Re-measured on 2026-11-04, Go 1.27.3 windows/amd64: the tree trial published, paged and
opened 1,000,000 entries in 52.6 s (within its 64 MiB heap-growth bound), and the row trial
validated, iterated and queried 1,000,000 rows in 71.5 s with a peak sampled heap of
53 MiB against a 256 MiB bound. One local run, not a performance guarantee.

## Cross-layer implementation checks

On 2026-11-04, the full normal and race-enabled suites, `go vet ./...`, and
`go build ./...` passed on Windows amd64. Continuous integration
(`.github/workflows/ci.yml`) now runs gofmt/vet/build/test on Linux, Windows and
macOS for every push, plus `-race` on Linux and Windows, in both modules; the
macOS leg is what first executes the Darwin lease and directory-sync paths in
this repository's automation (locally they had only been vetted and cross-built). Routine skips were the two opt-in large
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
