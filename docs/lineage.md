# Lineage

[Project](../README.md) · [Documentation index](README.md) · [Walkthrough](walkthrough.md)

Available Go API. Lineage records generic assertions and explicit evidence; it does
not execute activities or establish causal truth from matching content hashes.

## Purpose

Persist immutable provenance and independent revision histories, including bounded
row/column evidence and exact history-qualified references.

## Features

- Record generic activities, relationships, and independent revision histories with immutable caller-supplied IDs and idempotent retries.
- Link derived outputs to exact source revisions without merging their histories or inferring ancestry from equal content.
- Validate registered artifact references and stream explicit row/column/cell mapping evidence, including value and predicate/control dependencies; detailed selectors require a validator.
- Filter records by kind and reference role, and page over fixed snapshots with expiring cursors.
- Traverse explicit relationships, revision parents, and detailed mappings with depth, node, result, and work bounds; report truncation separately from evidence coverage.
- Persist records in an exclusive local journal, retain caller/adapter coverage assertions, and hide Internal support records from ordinary discovery.

## Mental model

Activities describe assertions about what happened. Revisions describe a history.
Derivation links can cross histories without merging them. A report derived from an
APK is not automatically the next APK revision. Identical artifacts may participate
in multiple unrelated histories because record IDs are supplied by callers, not
derived from output hashes.

## Responsibilities and boundaries

`Record` has a stable caller-supplied `ID` and exactly one payload:

- `Activity`: caller-defined Kind, optional Inputs/Outputs (`provenance.Ref`),
  Actor, ExternalSource, Metadata, and Evidence. Imports may have no inputs;
  annotations may have no outputs. Tool/version/parameters are ordinary optional
  metadata, never mandatory core fields.
- `Relationship`: caller-defined Kind, From/To references, optional establishing
  Activity ID, Metadata, and Evidence. For derived-from, From is the derived
  output and To its source. No content need be created to record a relationship.
- `Revision`: Artifact ID, existing parent revision IDs, optional establishing
  Activity ID, and Metadata. Parent links represent revision-of; that kind is
  reserved and cannot also be inserted as a Relationship. Existing-only parents
  prevent cycles on append. Multiple revisions may point at identical snapshots.

The store validates local artifact registration, revision/artifact binding, and
establishing activity references. Merely placing a manifest in CAS does not make
it a valid lineage artifact reference: authoritative catalog membership is required.
Actor/external links remain opaque external identifiers. Metadata is canonical JSON.
An identical canonical record/category retry returns the same ID; conflicting reuse
returns ErrConflict. Arrays retain their order as part of record identity.

Authoritative records are immutable JSON journal entries under an exclusive root
lease. Append publication and the in-memory record projection update synchronously;
a fresh query sees successful prior records. Opening the store replays the journal
to rebuild that projection. No background lag or eventual-consistency mode exists.
The current backend permits one live Service per root; close it before reopening.
A crashed owner's `.lease` requires explicit operator recovery after confirming no
owner remains. Directory entries are not synced; power-loss durability is unqualified.

## Public interfaces

- `New(artifactStore artifacts.Store, options Options) (*Service, error)`
- `(*Service).Close() error`
- `Record(ctx context.Context, record Record) (ID, error)`
- `Get(ctx context.Context, id ID) (Record, error)`
- `List(ctx context.Context, query Query) (Page, error)`
- `Traverse(ctx context.Context, query TraversalQuery) (TraversalPage, error)`

`Options` contains Root and `ValidateSelector func(context.Context, provenance.Ref)
error`. Supply Objects' ValidateRef for actual row/column membership validation.
Without it, detailed references fail with ErrUnsupportedSelector; lineage never
silently claims that a structurally plausible row exists.

## Working with other layers

Lineage and [Objects](objects.md) are peers above [artifacts](artifacts.md).
The shared `provenance` package contains neutral Ref, Dependency, Mapping, and
Coverage types, preventing circular Objects/lineage imports. CAS and storage know
nothing about SQL columns or revision histories. All APIs propagate context and
open lineage spans; supporting artifact/evidence lookup uses Internal context.

A Ref may name a whole artifact, a whole revision, a row, a column, or a cell.
Detailed refs require Artifact and Revision; columns/cells also require exact Schema.
Row-only graph identity ignores an optional Schema qualifier because its artifact
already pins the schema. Column/cell identity retains it. Display names, offsets,
and equal content alone do not identify version-scoped rows or schema columns.

For ordinary provenance, an activity with output artifact references may precede
its revision. For detailed output evidence, publish the output and record its
revision first, then record the activity/evidence and any establishing relationship.
That order avoids mutually forward-referencing activity/revision records. The
Revision.Activity field is optional; no later mutation is required.

## Examples

Given existing `artifactStore`, `objectService`, and a published `artifactID`:

```go
history, err := lineage.New(artifactStore, lineage.Options{
    Root: "./lineage-records",
    ValidateSelector: objectService.ValidateRef,
})
if err != nil { return err }
defer history.Close()
_, err = history.Record(ctx, lineage.Record{
    ID: "table-A/revision-1",
    Revision: &lineage.Revision{Artifact: artifactID},
})
```

A and B can retain independent histories when SQL produces C. Record C's own
revision, then record exact A/B/C revision references and mapping evidence for that
execution. A saved view pins its definition/inputs; actual row mappings describe a
specific execution, not just an unevaluated definition. Floating names must be
resolved before recording evidence. See the [walkthrough](walkthrough.md).

## Semantics, errors, and limits

### Direct queries and snapshots

Query supports Kind (`activity`, `relationship`, `revision`), ActivityKind,
RelationshipKind, Ref, Role, DependencyRole, establishing Activity, IncludeInternal,
Limit, and Cursor. Roles are `input`, `output`, `from`, `to`, `snapshot`, and `parent`.
A role requires Ref; incompatible filters are rejected. Empty role matches all
reference positions. DependencyRole selects `value`, `predicate`, or `control`
evidence. Producer/consumer lookup uses output/input rather than separate APIs.
Results are sorted by record ID. Returned records and pages are independent copies.

Limit defaults to 100 and caps at 1,000. Opaque cursors bind normalized filters,
bounds and a fixed immutable result snapshot; later appends do not enter subsequent
pages. Sessions expire after 15 minutes, are replayable until expiry, and do not
survive restart. Changing filters/bounds rejects the cursor. Shared pager quotas
are 128 sessions, 16 MiB per snapshot, and 64 MiB total; exhaustion is explicit.
Known-ID Get does not depend on a search cursor or derived query cache.

### Explicit row/column mappings

Individual row **and** column lineage is required. Evidence identifies an immutable
artifact/path containing NDJSON `provenance.Mapping` values, plus Coverage. Each
mapping has Output, Inputs (`Dependency{Ref, Role}`), and optional Expression text.
This supports many-to-many joins, aggregate-group membership, expressions, and
separate value versus predicate/control dependencies. Expressions are opaque adapter
explanations, not code interpreted by lineage. Cell references combine row+column.
No artifact-input list is expanded into invented detailed ancestry.

Mappings stream from artifacts; they are not embedded as one enormous record.
Limits per record: 1 MiB canonical record, 64 evidence references, 16 MiB total
mapping text, 1 MiB per line, 20,000 mapping lines/dependencies, and 10,000 inputs
per mapping. Expression text caps at 65,536 bytes. Evidence coverage explicitly
states complete/incomplete/unsupported for rows, columns, values, and predicates,
with optional bounded reasons. Lineage retains caller/adapter coverage assertions;
Objects must reject unsupported operators when complete evidence is requested.

Traversals report coverage relevant to visited mapping nodes separately from query
truncation. Complete evidence does not imply a bounded page is a complete graph.
Support mapping artifacts remain hidden in ordinary discovery, while explicitly
requested semantic row/column answers resolve their evidence without requiring
IncludeInternal merely to access support data.

### Traversal, filtering and failure outcomes

Traverse uses breadth-first search over explicit relationships, revision parents,
and mapping edges for detailed queries. Kinds defaults to derived-from; revision-of
is explicit. Direction is outgoing (default), incoming, or both. Outgoing follows
derived output to source or child revision to parents. Exact qualified references
are expanded once, and edges are emitted once in deterministic kind/identity order;
generic relationship cycles therefore terminate without merging histories.

Defaults/hard maxima: depth 4/32, nodes 1,000/10,000, results 2,000/20,000. Root counts
as a node. Page collections each remain <= Limit. Shared traversal budgets persist
across pages; truncation identifies depth/nodes/results rather than claiming a full
graph. Decoding/adjacency work additionally caps at 100,000 selected edges and a
32 MiB identity/expression estimate; exceeding it returns ErrLimit. The journal caps
at 128 MiB and 100,000 records. These are explicit bounded local-backend limits,
not an unbounded graph database promise.

Default List hides Internal records. Traversal also checks authoritative artifact
category and revision category, so a user relationship cannot expose a hidden bare
support artifact. Internal nodes may be resolved/expanded, but never collapse into
fabricated edges between visible nodes. IncludeInternal opts into diagnostics.
Classification is persisted at Record time and cannot change on retry.

RecordError carries ID, Committed and the underlying cause. A later cleanup error
can follow authoritative publication; callers must not infer rollback. ErrNotFound,
ErrInvalid, ErrConflict, ErrUnsupportedSelector and ErrLimit support errors.Is;
original causes remain wrapped. Cancellation is checked during validation, evidence
reads and traversal; it cannot interrupt arbitrary blocked I/O. Canceled/failed
pages are not returned as complete. Parent failures remain observable even when
supporting work is internal. Closed services reject further operations.

## Validation and evidence

Focused tests exercise the real storage/CAS/artifact stack: independent histories
sharing content, immutable idempotent records and conflicts, reopen/replay,
concurrent appends, snapshot paging under new commits, classification conflicts,
registered-reference enforcement, hidden bare artifacts, row/column join/group and
predicate evidence, selector mismatch/capability rejection, cycles, bounds,
cancellation and closed-cursor rejection. `go test -race ./lineage ./provenance`,
vet and build pass. No crash/power-loss or disk-full guarantees are claimed.
