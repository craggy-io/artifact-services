# End-to-end walkthrough

[Project](../README.md) · [Documentation index](README.md)

This is a design walkthrough, not executable CAS/Objects/lineage code. Only storage
and shared observability exist today. For runnable code, use the
[storage/observation example](observability.md#examples).

## 1. Ingest and name the bytes

An application receives an APK as a stream. In the proposed stack, CAS computes
its digest using bounded-memory staging, then publishes the complete bytes through
storage. Identical bytes can reuse verified CAS content. Storage only sees a safe
key and a stream: it does not know that the bytes are an APK or infer history.

Artifacts publishes a single-file manifest mapping `app.apk` to the CAS digest,
after verifying the referenced content. Successful publication also registers
catalog membership. The manifest represents the artifact; it is not a second
user-visible item. See [CAS](cas.md), [artifacts](artifacts.md), and the
[publication/visibility contract](queries.md#authoritative-facts-indexes-and-visibility).

## 2. Record an import without inventing a computation

A caller supplies a stable activity ID with kind `import`, no input artifacts,
and the new artifact as output. Actor/external-source references are optional.
An identical retry returns the record; conflicting ID reuse fails. An independent
import of identical bytes has a different activity ID and can establish a separate
history. Content equality alone does not prove which history was used.

Lineage records assertions and relationships; it does not execute the import.
If an analysis later produces a report, that report is derived from the APK rather
than automatically becoming the next APK revision. See [lineage](lineage.md).

## 3. Define and publish a typed table

A runtime schema describes columns, stable column identities, types, nullability,
and supported constraints. Objects validates the definition and stores schema S1
as an immutable artifact. An application uploads table data, then asks Objects to
validate its exact snapshot against S1 and pinned validator/encoding semantics.
Only the trusted typed-publication path may issue an accepted validated-type claim;
a generic metadata label is insufficient.

Validation may stream rows, but global constraints such as uniqueness require bounded
state or disk-backed work with explicit limits. A failure or incomplete check is
not success. The enforcement boundary and supported formats remain design decisions;
this walkthrough does not claim a validator or query engine is available.

## 4. Evolve schema and data without mutating either

Changing a column definition creates schema S2; S1 and data pinned to it remain
unchanged. Explicit schema-evolution mappings identify renamed, retained, or replaced
columns. A migration validates new data against S2 and publishes a new snapshot.
Generic lineage records the migration activity and exact old/new schema/data
references. The table's new revision links to its previous revision; the schemas
can maintain their own revision history. No mutable valid flag rewrites old data.

## 5. Combine A and B into C while retaining separate histories

Suppose table A has revision A7 and table/view B has revision B3. A query execution
pins A7 and B3, not just their content IDs. Any requested latest labels are resolved
before execution and those exact resolutions are recorded. A and B keep their own
histories; output C begins or continues a distinct history. Later C revisions link
to earlier C revisions, while derivation links cross to the selected A/B revisions.

A saved view stores its definition, adapter semantics/options and pinned inputs;
a materialized result stores the produced content. Actual row mappings describe a
specific execution, not merely the unevaluated saved-view definition. Transient
read-only results need not be persisted. Saving a result runs through typed
validation/publication and provenance recording, with partial-commit states reported
honestly rather than pretending these steps are one rollback transaction.

## 6. Explain rows and columns, not just input files

For a join producing C, the adapter supplies many-to-many row mappings and column
expression evidence. Aggregates identify contributing groups. Predicate/control
dependencies remain distinct from value derivation. Row identities are qualified
by exact object/history revision; columns also pin schema identity. Display names,
row positions, or equal hashes alone do not establish ancestry.

Compact immutable mapping artifacts and derived indexes support bounded, paginated
queries. Supporting records are internal in ordinary browsing, but explicitly
requested provenance answers still resolve them. Unsupported UDFs must report
incomplete/unsupported coverage or reject requests demanding complete detail.
See [required detailed lineage](lineage.md#semantics-errors-and-limits) and
[detailed provenance queries](queries.md#detailed-provenance-queries).

## 7. Discover and inspect only what is needed

Catalog search finds artifact IDs using typed filters over configured metadata;
lineage List/Traverse then follows selected references within explicit bounds.
Fetch only the needed manifests and content. SQL/table and JSON content adapters
interpret exact snapshots separately from catalog metadata search. Snapshot cursors
retain a fixed view; they do not promise a global transaction across every layer.
See [query contracts](queries.md).

## 8. Carry context and report honest outcomes

Attach observer/collector configuration once, then pass context through each scope.
Storage publication, CAS reuse, typed validation, artifact completion, and lineage
record acceptance are distinct outcomes. Mark support operations with WithInternal;
that classification is inherited by descendants. Default events/metrics hide internal
work, while parent duration remains inclusive and parent failures remain visible.
Requested semantic provenance answers belong to the user-facing query scope even
when resolving evidence requires internal work.

No layer should add child stream bytes to parent logical totals as unique payload.
Cancellation is cooperative; it cannot interrupt arbitrary blocked I/O or undo an
already committed publication. See [observability](observability.md) and the
[storage cancellation contract](storage.md#semantics-errors-and-limits).
