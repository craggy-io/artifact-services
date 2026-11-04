# Verification and measured evidence

[Project](../README.md) · [Documentation index](README.md) · [Walkthrough](walkthrough.md)

Requires Go 1.27 or newer, with no external dependencies.

```sh
go fmt ./...
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

This trial predates category filtering and is retained as a baseline, not a new
performance measurement of that change. It was not rerun for filtering: storage
streaming, cancellation, publication, and reader logic were unchanged; only event
selection and collector accounting changed, with focused/race coverage.

This is one local trial, not a storage benchmark guarantee. Read performance may
benefit from OS caching. HeapAlloc samples can miss transient peaks and exclude
process working set, native allocations and OS cache; they are not exact peak
memory measurements. Results do not qualify other mounts or crash durability.
