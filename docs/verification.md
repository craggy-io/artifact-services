# Verification and measured evidence

[Project](../README.md) · [Documentation index](README.md) · [Walkthrough](walkthrough.md)

Requires Go 1.27 or newer, with no external dependencies.

```sh
test -z "$(gofmt -l .)"
go test ./...
go test -race ./...
go vet ./...
go build ./...
```

Tests also cover default internal event/stat exclusion, diagnostic opt-in, sticky
classification across concurrent real storage operations, context isolation,
retained aggregate selection, and visible parent failures/inclusive duration.

Tests cover storage roundtrips, immutable/concurrent writes, failure cleanup,
invalid keys, source/OS error preservation, publication outcomes, context
inheritance, sibling linkage, per-layer counters, cancellation, and reader
lifecycles. Windows tests exercise Unicode/spaces, actual long-path roundtrips
(321-character root / 453-character blob path), invalid root characters, and
ACL-denied writes/reads in disposable fixtures. ACL tests skip only if icacls is
unavailable. Cleanup failures are narrowly fault-injected; disk-full, filesystem
sync/close failures, and crash recovery are not qualified by these tests.

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

This is one local trial, not a storage benchmark guarantee. Read performance may
benefit from OS caching. HeapAlloc samples can miss transient peaks and exclude
process working set, native allocations and OS cache; they are not exact peak
memory measurements. Results do not qualify other mounts or crash durability.

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

The local artifact/lineage journals require exclusive ownership of each root.
There is no distributed database, automatic stale-lease recovery, global
cross-layer transaction, arbitrary SQL engine, or crash/power-loss qualification.
Detailed lineage describes supported emitted-result dependencies, not why-not or
counterfactual explanations. Large-file measurements qualify streaming CAS/storage;
typed queries intentionally use separate bounded workloads.
