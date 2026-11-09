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
| [`watch`](#watch) | Follow every commit to the journals, once and in order, from a saved position |
| [`testkit`](#testkit) | Build a full stack in a test |
| [`sqlindex`](#sqlindex) | Query everything with read-only SQL (separate Go module) |
| [`postgres`](#postgres) | Keep every journal in PostgreSQL, shared by several processes (separate Go module) |
| [`gcs`](#gcs) | Keep blobs in a Google Cloud Storage bucket (separate Go module) |

## stack

`stack.Open(stack.Options{Root: dir})` creates `blobs`, `staging`, `catalog`, `lineage`
and `assertions` under one root, in dependency order, cross-wiring the validators so
that every reference form resolves. `Close` closes assertions, lineage and artifacts,
in that order. A root opened this way holds three leases, and all are released on
`Close` or process death. This is the wiring [AGENTS.md](../AGENTS.md) describes, in
executable form. Open one stack per independent root; the consumer decides how many.

`Options.Blobs` and `Options.Journals` replace the local blobs and journals with shared
backends, for a service that runs as several instances:

```go
journals, err := postgres.New(ctx, pool, postgres.Options{})
bucket, err := gcs.New(client, "craggy-models-blobs", "blobs/")
st, err := stack.Open(stack.Options{Root: localScratch, Blobs: bucket, Journals: journals.Journals()})
```

`Root` then holds only staging. [`stacktest`](../adapters/stack/stacktest/stacktest.go)
runs one suite over any pair of backends, including two stacks sharing one; see
[durability](durability.md#shared-backends) for what changes.

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

## watch

`watch` follows the artifact, lineage and assertion journals through their public
`Sequence`, `Changes` and `OnCommit`. It waits for commit notifications and never polls.

```go
from, err := watch.Current(ctx, watch.Of(st)) // or a saved Position; zero is the start
w := watch.Start(ctx, watch.Of(st), watch.Options{From: from}, func(ctx context.Context, b watch.Batch) error {
    // b.Journal names the journal; each b.Changes entry carries its Class, ID, Kind, Actor.
    return save(b.Position) // store it with the batch's effects
})
err = w.Stop() // before closing the stack
```

- **Once, in order.** Each journal's changes arrive in sequence order, in batches of at
  most `Options.Limit`. A drain is one catch-up from the watch's position, so coalesced
  notifications lose nothing and a burst of commits is one drain.
- **Across journals.** Journals share no sequence. A drain reports artifacts, then lineage,
  then assertions, each bounded by a tail read before the journals it can reference, so a
  record arrives after the records it references.
- **Restart.** `Batch.Position` is the position once that batch is handled. Store it with
  the batch's effects and pass it as `Options.From` to resume without repeats or gaps.
- **Failure.** A handler or read error ends the watch without advancing past the failed
  batch; `Stop` returns it. `Drain` reads once, to the tails at the call, without following.
- `Options.IncludeInternal` reports internal records; left out, they still advance the
  position. `Journals` can name any subset, or other implementations of `Journal`.

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

## postgres

`github.com/craggy-io/artifact-services/adapters/postgres` keeps every journal root as
rows in one PostgreSQL database, so any number of processes can serve one stack. `New`
creates its two tables if missing (safe to run from several processes at once).
`Options.Prefix` namespaces root names, so several stacks can share a database. The caller
owns the `pgxpool.Pool`: on Cloud SQL, connect with the Cloud SQL Go connector and IAM
database authentication. Size the pool for one held `LISTEN` connection plus a commit and
a read per journal root: at least six for a full stack. `Backend.Close` stops the listener
after the stacks close. Commit semantics are in [durability](durability.md#shared-backends).
Its tests need `ARTIFACT_SERVICES_POSTGRES_DSN` and fail without it.

## gcs

`github.com/craggy-io/artifact-services/adapters/gcs` is a `storage.Store` over one bucket,
with `Stater` and `Reclaimer` so `gc` works. `Put` creates an object only if none exists,
mapping the precondition failure to `ErrAlreadyExists`, and an interrupted upload never
becomes visible. A `Put` that finds the key refreshes the object's custom time, and
`Remove` deletes conditionally on the generation it checked, so a blob in reuse survives a
concurrent sweep. It is not `Seekable`: `cas.OpenSeekable` returns `ErrCapability`, so use
[`spool`](#spool). Its tests need a GCS emulator at `STORAGE_EMULATOR_HOST`.
