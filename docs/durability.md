# Durability and crash recovery

[Project](../README.md) · [Documentation index](README.md) · [Verification](verification.md)

What a successful return promises, what happens if the process or machine dies, and how
to compose layers without a shared transaction.

## What success means

By default, a returned success means the write was flushed to stable storage on a local
filesystem. That covers process death, OS crash and power loss, as far as the operating
system and hardware honour a flush.

| Stage | What happens, in order | Commit point |
| --- | --- | --- |
| Storage `Put` | Write temporary file, fsync it, hard-link to the final name, flush the name | The link. A flush failure after it returns `PutError{Published: true}` |
| CAS `Put` | Hash into an unsynced staging file, then storage `Put` | Storage's link. Staging is a disposable copy |
| Artifact `Publish` | Verify content, store manifest blocks and root through CAS, commit a journal record | The journal record. `PublishError` distinguishes the stages |
| Journal commit | Write one checksummed frame, fsync the log, update indexes, notify | The fsync. On failure the log is truncated back, so an error means absent |
| Journal creation | Fsync the header and config, then flush their names | Once per root |

Flushing a name means a directory fsync on Unix, up to the store root. On Windows, where
directory handles cannot be flushed, it means `FlushFileBuffers` through the new name,
which commits the NTFS log containing the link.

`Options.Relaxed` on artifacts, lineage and assertions skips the per-commit log fsync.
Commits then survive process death but not an OS crash or power loss. The test kit uses
it for speed. Storage has no relaxed mode.

If truncating after a failed commit also fails, the on-disk tail is unknown, so the
journal refuses further commits until it is reopened.

## Recovering from a crash

Reopen. There is no recovery operation.

- **Leases.** A root's lease is an OS file lock, which the kernel releases when the owner
  dies, so a dead owner cannot block anyone and there is nothing to reclaim. The `.lease`
  file is never deleted, and do not delete it yourself. A *live* owner still excludes
  others, with `ErrLeaseHeld` and a `LeaseError` naming its PID, host and start time. Those
  are diagnostic only and never decide exclusion.
- **Torn tails.** A frame that fails its checksum at the end of the log was never
  acknowledged, and is truncated on open. Damage anywhere else is `journal.ErrCorrupt`.
- **Leftovers.** A crash can leave temporary and staging files and unreferenced content.
  These are never visible as results. [`gc`](adapters.md#gc) accounts for and reclaims them.
- **Unfinished work.** A run or session that died has a start record and no end record.
  `runs.Coordinator.Unfinished` reports it. Nothing tidies it up, so the record stays true.

## Composite operations

The layers have no shared transaction, and this is not one. Each step instead has a stable
ID and is idempotent, so the recovery from a failure anywhere is to run the same composite
again.

| Layer | Stable identity | Identical retry | Different content | Stage error |
| --- | --- | --- | --- | --- |
| CAS | Content digest | Reuses | Not possible | `cas.PutError.Committed` |
| Artifacts | Content-derived ID | Reuses | Not possible | `artifacts.PublishError` |
| Occurrences | Caller's `OccurrenceRequest.ID` | Reuses | `ErrConflict` | `artifacts.OccurrenceError.Committed` |
| Lineage | Caller's `Record.ID` | Reuses | `ErrConflict` | `lineage.RecordError.Committed` |
| Assertions | Caller's `Request.ID` | Reuses | `ErrConflict` | `assertions.RecordError.Committed` |

Use this order, which `runs` implements:

1. Start record. From here, dying leaves the work visibly unfinished.
2. Store content. Register nothing.
3. Publish snapshots. Nothing names them yet, so they are not a result.
4. Commit the completion record. This is the single commit point. It names the outputs
   and everything still to be derived from them.
5. Record occurrences, derivation edges and planned assertions from the completion record.
   Anyone who finds the record can redo this step.

Record things before anything that references them. References are checked at record
time, so a wrong order fails instead of dangling.

**What can be seen partway through.** These windows are the whole of the partial
visibility, and they are safe to observe:

- Between 3 and 4, published snapshots appear in raw catalog `Search`. No occurrence or
  lineage names them. Treat a snapshot as a result only through its occurrence or record.
- Between 4 and 5, a completion record exists with some of its occurrences, edges or
  assertions missing. Rerunning or any later reuse fills them in.
- Each journal has its own sequence. There is no sequence across journals, and a query's
  consistency is local to that query.

The same reasoning covers `objects.Materialize`, which publishes results and evidence and
leaves lineage to the caller.

## Limits

Kill tests prove what a process death leaves behind and that reopen and rerun converge.
They cannot prove a flush reached the platter: power loss has not been physically tested,
and depends on the OS, filesystem and drive honouring flushes. Network filesystems are not
qualified, and locks and hard links there vary. One live process owns each journal root,
so a separate read-only inspection process cannot open a root beside its writer. It can
read an [exported index](adapters.md#sqlindex) instead.

See [verification](verification.md#crash-qualification) for what ran.
