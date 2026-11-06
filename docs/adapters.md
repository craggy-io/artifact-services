# Adapters

[Project](../README.md) · [Documentation index](README.md) · [Durability](durability.md)

Optional packages under `adapters/`, composed only from public core APIs. They hold
the opinions the core should not have, such as a SQL engine or a run protocol. None
has privileged access, so anything here could be rebuilt by a consumer.

| Package | Use it to |
| --- | --- |
| [`stack`](#stack) | Open and close every layer with correct wiring |
| [`runs`](#runs) | Run a function and record it: sessions, pinned reads, deterministic reuse |
| [`typed`](#typed) | Use Go types instead of raw JSON for assertions and table rows |
| [`treeio`](#treeio) | Move directories and files in and out of artifacts |
| [`spool`](#spool) | Get a seekable, verified file from any store |
| [`gc`](#gc) | Find and reclaim unreferenced content, and verify a stack |
| [`testkit`](#testkit) | Build a full stack in a test |
| [`sqlindex`](#sqlindex) | Query everything with read-only SQL (separate Go module) |

## stack

`stack.Open(stack.Options{Root: dir})` creates `blobs`, `staging`, `catalog`, `lineage`
and `assertions` under one root, in dependency order, cross-wiring the validators so
that every reference form resolves. `Close` closes assertions, lineage and artifacts,
in that order. A root opened this way holds three leases, and all are released on
`Close` or process death. This is the wiring [AGENTS.md](../AGENTS.md) describes, in
executable form. Open one stack per independent root; the consumer decides how many.

`Stack.PublishTable(ctx, memo, def, rows)` publishes rows as a table artifact validated
against `def`, defining the schema once per assertions service and memo key (a changed
definition under one key fails). `TableSchema(key, cols...)` builds a table definition
whose key columns are non-nullable and every other column nullable.

## runs

`runs` coordinates records around a function you supply. It executes nothing itself,
sandboxes nothing, and does not make an untrusted function safe.

```go
coordinator, err := runs.New(runs.Options{CAS: st.CAS, Artifacts: st.Artifacts, Lineage: st.Lineage, Assertions: st.Assertions})
session, err := coordinator.Begin(ctx, runs.SessionRequest{ID: "s1", Actor: "someone"})
result, err := session.Run(ctx, runs.Job{
    ID:            "convert-1",
    Code:          runs.Code{Kind: "builtin", Name: "convert", Version: "1"},
    Inputs:        []provenance.Ref{{Occurrence: "input"}},
    Deterministic: true,
}, func(ctx context.Context, run *runs.Run) error {
    return run.Blob(ctx, "out.txt", strings.NewReader("output"), 0o644)
})
err = session.End(ctx, err)
```

**Sessions** nest through `Child`. A start record is written first, and a session or
run that dies has a start and no end, which `Coordinator.Unfinished` reports.

**Outputs** stay private during the function: `Blob`, `Tree` and `Content` store
content but register nothing. On failure, only outputs marked with `Keep` are
published, returned on `RunError.Outputs`. `PlanAssertion` records an assertion about
an output only if the run succeeds.

**Reads** through `Run.Open`, `Run.Head` and `Run.Assertions` resolve as of the
sequences pinned at start (`Run.Pins`), and each read becomes a recorded input. `Strict`
rejects reads of undeclared inputs with `ErrUndeclaredRead`. A deterministic job is
always strict. This is advisory bookkeeping, not a security boundary: the function can
hold other handles.

**Deterministic reuse** computes a key from the code identity, canonical parameters and
input content digests (`DerivationKey`). If a completed run with that key exists, its
outputs are returned with `Result.Reused` set and the function is not called. Whether
reuse is sound for a given job is the caller's judgement. Rerunning a run ID that already
ended in failure returns `ErrPriorFailure`; use a new ID to try again.

Racing executors each run the function; exactly one commits. The loser adopts the
winner's outputs and reports reuse. No extra core primitive was needed, because the
completion record has an ID fixed by the key and journal key uniqueness is the claim. A
loser's differing outputs stay as unreferenced snapshots: no occurrence or edge names
them, but raw catalog `Search` can see them.

The commit order is in [durability](durability.md#composite-operations).

## typed

`DefineKind[T]` derives a shape from a struct (or takes one explicitly) and returns a
`Kind[T]` with `Claim`, `About`, `Current`, `Keyed`, `Citing` and `Get`, each returning
`Said[T]`. Options are `WithKey`, `WithEvidence`, `WithWriter`, `Replacing`, `WithActor`,
`WithActivity` and `WithID`. `DefineTable[T]` does the same for tables, with `Put`,
`RowID` and `Rows[T]`. Types `ShapeOf` cannot represent return `ErrUnsupportedType`.

## treeio

`Ingest` walks a directory or file into CAS and an artifact, preserving modes, and can
record an occurrence with a `Source` reference. `IngestReader` does the same for one
stream. `Materialize` writes an artifact to a directory with modes restored, using
`os.Root` so no entry can leave it (`ErrEscape`). Policy is explicit: `Exclude`, `Mode`,
and `Symlinks` (`SymlinkReject` by default, `SymlinkSkip`, `SymlinkFollowInside`).

There is deliberately no download source, because the module does no network I/O. Fetch
it yourself, then pass the reader and an external reference to `IngestReader`.

## spool

`spool.OpenFile` and `spool.Copy` stream content to a temporary file through the
verifying reader, hand back a seekable file only after the digest matches at EOF, and
remove the copy on `Close`. Use it for backends without
[`OpenSeekable`](artifacts.md#seekable-access), or when verification must complete
before processing. The cost is a full copy each time.

## gc

- `Scan` reports orphans and missing content. It changes nothing.
- `Sweep` removes orphans older than `SweepOptions.Horizon`, which is required. `DryRun`
  reports without removing.
- `Verify` re-hashes reachable content and checks that journals, catalog and references
  agree, returning `Findings`.

Sweep relies on two conditions, neither involving journal sequences. `artifacts.Fence`
excludes every publication's check-then-register during reachability and removal. The
horizon spares anything written or re-Put recently, protecting work between `Put` and
`Publish`, so it must exceed the longest such gap in your application. Too short a
horizon costs a failed, retryable publication, never a dangling reference. Nothing here
removes a registered artifact; retention remains consumer policy.

## testkit

`testkit.Open(t)` opens a full stack in `t.TempDir()` with cleanup registered, and
provides `Reopen`, `Runs`, `Session`, `Put`, `File` and `IDs`. It defaults to `Relaxed`
for speed, so durability tests should use `stack` directly.

## sqlindex

A rebuildable SQLite read model in a **separate Go module**,
`github.com/craggy-io/artifact-services/adapters/sqlindex`, so the core keeps no external
dependencies. It uses the pure-Go `modernc.org/sqlite`. Until the module is tagged, its
`replace` directive means a consumer must add its own `replace` or pin.

- `Open`, `Sync` and `Follow` read the journals' change feeds into the index, resuming
  from `Positions`. `Rebuild` starts over; the index is never authoritative.
- `Query`, `Rows` and `ScanRows[T]` run one `SELECT` under row and time limits. Exceeding
  the row limit returns `ErrRowLimit`, never a silently short result. Other statements,
  including `ATTACH`, are `ErrReadOnly`, and the connection itself is read-only.
- `MaterializeTable` copies a typed table's rows in on demand.
- `Export` and `PublishSnapshot` produce a standalone copy, labelled with its positions.

Results are as of `Positions`, not a transaction across journals. The tables are in
[schema.sql](../adapters/sqlindex/schema.sql). A consumer with many queries depends on
those names, so the schema is versioned API: `SchemaVersion` changes when a name changes,
and `Open` refuses other versions with `ErrSchemaVersion`. The fix is to rebuild.
