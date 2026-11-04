# Lineage

[Project](../README.md) · [Documentation index](README.md) · [Walkthrough](walkthrough.md)

Proposed API design only; this package is not implemented. Examples are illustrative, not runnable APIs.

## Purpose

Persist generic provenance and independent revision histories, including required row/column evidence.

## Mental model

Activities describe assertions about what happened; revisions describe history; derivation crosses histories without merging them.

## Responsibilities and boundaries

The lineage methods belong to a proposed store interface; `Record` is also
the package's proposed record type. It contains a caller-supplied stable `ID` and
exactly one payload: `Activity`, `Relationship`, or `Revision`. This tagged record
model gives all record kinds the same persistence, lookup, retry, and query rules
without requiring a separate CRUD API for each. Referenced proposed types are:

- `Activity`: caller-defined `Kind`, optional `Inputs`/`Outputs` as lists of Ref,
  optional actor/external-source references, and caller metadata. Ref identifies
  an `artifacts.ID`, a lineage revision ID, or a version-qualified Objects
  sub-object selector as described in [detailed provenance](lineage.md#semantics-errors-and-limits). Import may have no inputs;
  annotation may have no outputs. Tool, version, and parameters are optional
  metadata, never mandatory core fields. An activity records provenance; recording
  it neither starts execution nor asserts that a tool ran successfully.
- `Relationship`: caller-defined `Kind`, `From` and `To` Ref, optional establishing
  activity ID, and metadata. For `derived-from`, From is the derived artifact or
  revision and To is its source. A relationship can be recorded without producing
  new bytes or even recording a new activity.
- `Revision`: an `artifacts.ID` snapshot, zero or more parent revision IDs, an
  optional establishing activity ID, and metadata. Parent links represent
  **revision-of**; that reserved relationship is expressed only here, not duplicated
  as a generic Relationship. Activity/relationship records may reference bounded
  mapping evidence and coverage rather than embedding enormous edge lists.
  Distinct revision IDs can point at the same snapshot
  while recording different histories. A report derived from an APK is a derivation,
  not automatically an APK revision. Parent links are explicit assertions by the
  caller, not inferred from digest equality.
- `Query`: optional record-kind, activity-kind, relationship-kind, and referenced
  Ref filters, plus bounded `Limit` and opaque `Cursor`. Reference matching includes
  activity inputs/outputs, relationship endpoints, and revision snapshot/parents;
  results retain these roles so callers can distinguish provenance from downstream
  uses. `Page` contains `Records []Record` and `NextCursor`. Pagination requires a
  stable snapshot as defined by the [query contract](queries.md). Reference-role filters
  distinguish producers, consumers, endpoints, and revision parents without
  introducing separate specialized lookup methods.

Record validates and persists immutable records, returns their ID, and requires
referenced local artifacts/revisions/activities to exist. Parent references must
be existing revisions, preventing cycles on append; external/actor references are
opaque metadata links, not required local records. Querying by a source finds
consuming activities, derivation edges, and child revisions; Get follows their
explicit references to retrieve the corresponding output/provenance records.
To establish a revision with an activity, first record the activity with its
already-published output artifact reference, then the revision referencing that
activity. This avoids requiring mutually forward-referencing records.

IDs are scoped to the lineage store and supplied independently of output hashes.
An identical normalized retry under the same ID returns that record; conflicting
reuse errors. Independent activities with identical outputs retain distinct IDs.
Changing provenance creates another immutable record rather than overwriting one.
The layer validates structure/references and stores caller assertions; it cannot
prove causal truth from matching hashes. Mutable labels, branch heads, and other
movable references are separate from immutable records, with their API deferred.
No execution engine or automatic branch/merge policy is implied.

## Public interfaces

- `Record(ctx context.Context, record Record) (ID, error)`
- `Get(ctx context.Context, id ID) (Record, error)`
- `List(ctx context.Context, query Query) (Page, error)`
- `Traverse(ctx context.Context, query TraversalQuery) (TraversalPage, error)`

## Working with other layers

References [artifacts](artifacts.md) and selector contracts owned by [Objects](objects.md). It does not execute activities or interpret SQL inside CAS/storage.

## Examples

See the [walkthrough](walkthrough.md) for A/B/C independent histories, exact input revisions, and migration relationships.

## Semantics, errors, and limits

Individual row **and** column lineage is a core Objects/lineage contract, not an
optional artifact-level approximation. An adapter must provide actual mapping evidence;
a list of input artifact IDs cannot establish detailed ancestry. Capability reporting
may explicitly declare unsupported operations, but cannot claim complete row/column
support by returning only whole-artifact dependencies.

Sub-object Ref selectors are defined/versioned by Objects and carried by generic
lineage. A row reference qualifies an immutable object/data artifact and exact
history revision with a version-scoped stable row identity; a column reference
qualifies that snapshot/history, pinned schema revision, and stable schema column
identity. Offsets, current column names, or content hashes alone are insufficient.
Identifiers must distinguish duplicate-valued rows and repeated uses of identical
snapshots in different histories. Renames/reorders do not create implicit ancestry;
identity preservation or replacement across revisions requires explicit mappings.
Generic lineage validates reference structure/ownership through the selector contract,
without embedding SQL/table meaning in CAS or artifacts.

The evidence model must represent many-to-many row mappings for joins, column
expression dependencies, aggregate output dependence on contributing groups, and
filter/predicate/control dependencies separately from value derivation. Selecting
one input row is not necessarily the same as deriving its values. When row and
column ancestry intersect, the reference model must permit a row+column cell selector;
cell-level dependency expansion/completeness rules remain an explicit design decision,
not an implemented promise of arbitrary cell tracing.

For example, A has its own revision history and B (a table or saved view) has a
separate history. SQL producing C records exact A/B input revisions and detailed
mapping evidence. C has its own revision history: derived-from links cross histories,
while revision-of links connect successive C revisions. The operation must not merge
A/B histories or treat C as their next revision. Even if content IDs match across
histories, the selected revision references preserve which inputs were actually used.
A saved view can describe potential dependencies; actual row mappings belong to a
specific execution over pinned inputs, not to the unevaluated definition alone.

Large provenance is persisted in compact immutable mapping artifacts referenced by
activity/relationship evidence, with derived adjacency indexes for lookup. Do not put
every edge into RAM or a single unbounded lineage record. Representations can encode
row ranges, join/group membership and expressions compactly, but compression must not
invent stronger coverage. Mapping evidence binds exact input/output revisions, selector
and adapter semantics, dependency roles, and coverage. Persistence/encoding and stable
row-ID allocation policy remain to be specified, including how transformations assign
new output identities and how schema evolution maps column identities.

Every operation/result exposes coverage for rows, columns, dependency roles, and
relevant operator scopes: complete, incomplete with gaps/reasons, or unsupported.
Opaque UDFs and unsupported engine operations must either report those limitations
explicitly or reject a request requiring complete detailed lineage. Completeness
requirements are checked both before execution against capabilities and at completion
against actual evidence. Early termination, failed capture, or bounded/truncated
answers cannot be labeled complete. A snapshot with partial evidence may be saved
only under an explicit permissive request and must retain its incomplete coverage;
strict requests cannot publish it as a complete-provenance result.

Direct filters, traversal direction/bounds, cursor consistency, authoritative persistence and evidence visibility are defined in [query contracts](queries.md).

## Validation and evidence

No runtime qualification exists for this proposed layer. The contracts here are design requirements, not test results. Open implementation/encoding decisions remain explicit in this guide and the [query contracts](queries.md).
