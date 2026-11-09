# 0001: Shared journal and blob backends

## Status
Accepted — 2026-11-09, by the user's advance acceptance of the cloud-first plan's ADRs
("Accept in advance") in the session that implemented it. Part of the Craggy cloud
hosting pattern, `craggy-cloud` ADR 0001.

## Context
Craggy apps will run on Cloud Run: stateless, scaled to zero, and with several instances
at once. Every journal root here was a local file under an exclusive OS lease, so exactly
one live process could serve a stack. Notebook-services relies on guarded publication
(its ADR 0010): of two writers that started from the same head, exactly one may become
current. That guarantee has to hold across instances.

## What the journal promised (the seam)
Read from `internal/journal` and every caller before the change; each item is now a
`backend.Log` obligation, checked by `backend/backendtest`:

- **Key lookup.** One record per key. Committing an identical record again reuses it
  (`reused`), a different one is `ErrConflict`. The comparison is on canonical JSON.
- **Guards.** `Txn.Check` runs inside the commit critical section, after the idempotency
  decision, so a replayed identical record reuses without being re-checked, and of two
  racing guarded commits exactly one passes. `Txn.Apply` updates derived state once the
  record is durable and must not fail.
- **Sequence numbering.** Contiguous from 1 per root, equal to commit order, surviving
  restart. The change feed and `adapters/watch` resume from a sequence, so a gap or a
  reordering would lose changes.
- **Replay.** Opening rebuilds every layer's in-memory index by scanning the journal in
  order. Values are read back on demand (`Get`, `At`, `Scan`).
- **Change pages.** `Changes` reads after a position, skipping filtered records but
  advancing `Last` over them.
- **`OnCommit`.** A coalesced wake-up after each durable commit, never blocking a commit.
  Not a delivery log.

## Decision
1. **A public `backend.Log` seam** beneath every journal, with exactly those semantics, and
   the existing file journal as its local implementation (`internal/journal`'s file log,
   still leased, still the default). No other options were added.
2. **`journal.Store` orchestrates over any log.** Derived state sees every record exactly
   once, in order: records it commits through `Txn.Apply`, all others through a layer
   `Follow` hook (every record on open; other processes' records on catch-up).
3. **Catch-up rule.** Every layer read starts with `CatchUp`: one query for the log's latest
   sequence, then the missing records through `Follow`. A read therefore sees every commit
   that finished before it started, from any process (read-your-writes across instances).
   Inside a commit, the store catches up to the locked latest sequence before running the
   guard, so guards see the true head.
4. **PostgreSQL journals** (`adapters/postgres`, its own module). One `roots` row per root
   holds its config and latest sequence; `entries` has the sequence as primary key, plus
   the key (unique per root) and the canonical bytes. A commit is one transaction that
   locks the root's row: commit order equals sequence order. The guard runs inside that
   transaction as a compare-and-set. `LISTEN/NOTIFY` (payload: the root) wakes other
   processes, which `Scan` from their last sequence. Transactions replace local file locks
   for this backend; there are no leases.
5. **GCS blobs** (`adapters/gcs`, its own module). `Put` uses `ifGenerationMatch=0`; a 412
   maps to `ErrAlreadyExists`. It implements `Stater` and `Reclaimer` (conditional delete on
   the generation and metageneration checked) but not `Seekable`; `cas.OpenSeekable` then
   returns `ErrCapability`, and callers spool. Blobs are written before the journal
   transaction that references them, so metadata never names a missing blob.
6. **`adapters/stack`** opens either: `Options.Blobs` and `Options.Journals`, local when nil.
   The core module gains no dependencies.

## Consequences
- The local backend keeps every guarantee and its exact on-disk format.
- Commits serialise per root, which caps write throughput per stack. That is fine at
  Craggy's scale; shard by root (one per notebook or user) if it ever matters.
- A cold start replays the whole journal. Measure it (cloud-first plan, Phase 4); snapshot
  indexes to the bucket only if it passes about two seconds.
- A failed `COMMIT` without a server error is resolved by reading the record back; if that
  also fails the outcome is unknown, reported as `CommitError{Committed: false}`, and an
  identical retry reuses the record if it exists.
- `Options.Relaxed`, `MaxRecords` and `MaxBytes` configure only the file log.
- Pools must allow one listener plus a commit and a read per root (six for a full stack).
  Size them with Cloud Run's max instances against Cloud SQL's connection limit.
- Tests: `backend/backendtest` runs on both logs and on its in-memory shared log
  (`Memory`, for consumers' multi-process and point-in-time restore tests), `adapters/stack/stacktest` on local,
  GCS and PostgreSQL backends, including two stacks on one database racing guarded commits
  for ten rounds. The server-backed tests fail rather than skip without their servers.

## Alternatives
- **Firestore journals.** Free at rest, but no cheap single ordered sequence per root and no
  SQL search.
- **SQLite on a VM** (craggy-data's model). Does not work with several Cloud Run instances.
- **Keep one writer instance** (max instances 1). Simple, but every request then queues on
  one instance and a deploy has a gap; it also hides, rather than solves, cross-process
  correctness.
