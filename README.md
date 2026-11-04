# artifact-services

A streaming Go library. **Objects and lineage are peers over artifacts; artifacts → CAS → storage.**
All layers report their own work through shared context-based observability.
Storage and observability exist; the CAS, artifacts, Objects, and lineage signatures below
are proposed API design and are not yet available. There is no server or CLI.

| Layer | Responsibilities | Public interfaces |
| --- | --- | --- |
| `storage` | Physical layout, streaming I/O, existence checks, atomic publication, and explicit storage observation/cancellation. | <ul><li><code>NewFilesystem(root string) (*Filesystem, error)</code></li><li><code>Put(ctx context.Context, key string, source io.Reader) error</code></li><li><code>Open(ctx context.Context, key string) (io.ReadCloser, error)</code></li><li><code>Exists(ctx context.Context, key string) (bool, error)</code></li></ul> |
| `observability` | Shared context configuration, parent-child scopes, throttled stream progress, semantic counters, and category-filtered process-local aggregation by layer/operation. | <ul><li><code>With(ctx context.Context, options Options) context.Context</code></li><li><code>WithInternal(ctx context.Context) context.Context</code></li><li><code>Start(ctx context.Context, scope Scope, key string) (context.Context, *Span)</code></li><li><code>(*Span).AddBytes(n int64)</code></li><li><code>(*Span).Count(name string, n int64)</code></li><li><code>(*Span).RecordError(err error)</code></li><li><code>(*Span).End(err error)</code></li><li><code>(*Collector).Snapshot(options SnapshotOptions) map[Scope]Metrics</code></li></ul> |
| CAS (`cas`) | Immutable bytes by digest, verified streaming reads, and logical content deduplication. | <ul><li><code>Put(ctx context.Context, source io.Reader) (PutResult, error)</code></li><li><code>Open(ctx context.Context, id ID) (io.ReadCloser, error)</code></li><li><code>Exists(ctx context.Context, id ID) (bool, error)</code></li></ul> |
| `artifacts` | Immutable snapshots referencing CAS content, bounded file listing, and typed catalog discovery. | <ul><li><code>Publish(ctx context.Context, manifest Manifest) (ID, error)</code></li><li><code>Get(ctx context.Context, id ID) (Manifest, error)</code></li><li><code>OpenFile(ctx context.Context, id ID, path string) (io.ReadCloser, error)</code></li><li><code>ListFiles(ctx context.Context, id ID, query FileQuery) (FilePage, error)</code></li><li><code>Search(ctx context.Context, query CatalogQuery) (CatalogPage, error)</code></li></ul> |
| `objects` (Objects) | Runtime schemas, immutable typed snapshots, validation, content-query adapters, and required row/column provenance evidence. | <ul><li><code>DefineSchema(ctx context.Context, definition SchemaDefinition) (artifacts.ID, error)</code></li><li><code>GetSchema(ctx context.Context, id artifacts.ID) (SchemaDefinition, error)</code></li><li><code>Validate(ctx context.Context, request ValidationRequest) (ValidationReport, error)</code></li><li><code>Publish(ctx context.Context, request PublishRequest) (artifacts.ID, error)</code></li><li><code>Query(ctx context.Context, request QueryRequest) (ResultStream, error)</code></li><li><code>Capabilities(ctx context.Context) (Capabilities, error)</code></li></ul> |
| `lineage` | Independent revision histories and generic provenance, including version-qualified row/column references and bounded mapping traversal. | <ul><li><code>Record(ctx context.Context, record Record) (ID, error)</code></li><li><code>Get(ctx context.Context, id ID) (Record, error)</code></li><li><code>List(ctx context.Context, query Query) (Page, error)</code></li><li><code>Traverse(ctx context.Context, query TraversalQuery) (TraversalPage, error)</code></li></ul> |

## Layer design

Table signatures are package-local interface methods unless explicitly shown as
functions/receiver methods for existing APIs; proposed backend constructors and
persistence formats are intentionally not specified. Every domain operation takes
context first. Existing constructors, context attachment, and span/collector methods
retain their actual signatures: spans already carry the operation configuration.

### Storage

`storage.Store` owns physical placement, streams, existence checks, and immutable
atomic publication. It accepts opaque safe keys, not filenames or content-identity
claims. CAS supplies digest-derived keys; storage neither hashes nor deduplicates
logically. The filesystem constructor chooses the root, including a mounted volume.
The detailed error, cancellation, and publication contract below remains authoritative.

### CAS

`cas.ID` is an algorithm-qualified content digest (initial algorithm: SHA-256).
`PutResult` contains `ID`, `Size int64`, and `Reused bool`. Put consumes a stream,
computes its identity, and either publishes the immutable bytes or reuses verified
existing content. It does not close the caller's source. Because storage needs a
key before writing, CAS must hash while spooling to bounded-memory temporary
storage, then publish from that spool; this is not a whole-payload RAM buffer.
CAS owns cleanup of its staging data and preserves committed-with-error outcomes.
A published ID remains available in the result when a later cleanup error occurs.

Open returns a caller-owned streaming reader that verifies the requested digest.
Successful full integrity verification requires reading through EOF; a digest
mismatch replaces successful EOF with an integrity error. Early Close releases
resources without claiming verification or draining the remaining content. Bytes
can be delivered before final verification, so callers requiring verified input
before use must stage it themselves. Exists checks presence only, not integrity.
Before reporting Reused, Put verifies the existing content, including when another
writer wins publication; corrupt existing data is an error, never silently reused
or overwritten. CAS has no filenames, revisions, or provenance. Reused bytes are
not an activity identity and do not establish a relationship between artifacts.

### Artifacts

`artifacts.ID` identifies a canonical, versioned manifest by content. `Manifest`
contains `Entries []Entry` and `Metadata map[string]json.RawMessage`; each Entry
contains a normalized relative `Path`, a `cas.ID` content reference, and `Size
int64`. One entry represents a single-file snapshot; multiple entries represent a
collection. Paths are unique, case-sensitive slash-separated names: absolute
paths, backslashes, empty segments, and dot/traversal segments are rejected.
These are logical paths, not permission to extract directly onto a host filesystem.

Publish validates the manifest, checks each referenced CAS blob for availability,
size and integrity, then publishes the canonical manifest atomically through CAS.
Only after that publication and durable catalog registration (defined below) may
it report successful completion of the snapshot/collection. Missing,
corrupt, or mismatched references prevent completion. Canonicalization includes
sorted entries, normalized metadata encoding, and a schema version so equivalent
manifests have stable IDs; these encoding rules must be specified before coding.
Metadata changes affect snapshot identity. Unchanged file bytes reuse CAS content.
A post-publication cleanup error must still identify the committed snapshot ID.

Get returns the bounded manifest; OpenFile resolves a manifest path and delegates
to CAS's verified streaming read contract. Payloads are uploaded through CAS before
Publish, so no API requires all file bytes in memory. Manifest entry/metadata limits
must be enforced on both publication and decoding; exact limits are a later choice.
A bounded in-memory manifest is acceptable, while arbitrarily large manifests are
not implicitly promised. Artifacts contain immutable snapshots, not revision order,
mutable names, or a causal account of how their content was obtained.

### Objects

Objects gives typed meaning to immutable artifacts; artifacts provide snapshots,
and lineage provides histories and relationships. Objects and lineage are peers
above artifacts, not a single linear stack. A higher-level coordinator composes
schema validation, typed publication, and provenance recording without making
artifacts depend on Objects or forcing circular package imports. No SQL engine,
dialect, JSON query language, table encoding, or executable validator is selected.

**Runtime definitions and validation.** `SchemaDefinition` is a bounded declarative
document with a definition-format/version and data model. Table definitions specify
stable column identities, display names, value types, nullability, and constraints;
JSON definitions specify fields, nesting, required properties, types, and constraints.
Logical shape is separate from physical encoding. Users define types at runtime
without compiling Go types or uploading executable validation code.

DefineSchema validates a definition before storing it as an immutable schema
artifact; GetSchema retrieves that exact definition. `ValidationRequest` identifies
an immutable data artifact, exact schema artifact ID, physical encoding, validator
semantics/version, and resource limits. `ValidationReport` binds those exact
identities and reports validity, checked constraint coverage, and bounded diagnostics;
errors/incomplete checks must not be represented as valid. Large tables stream rows
where possible. Uniqueness and other global constraints require bounded state or
disk-backed execution with explicit limits; exceeding limits fails validation rather
than silently omitting constraints. JSON streaming limits depend on the chosen
encoding and constraints, and are not assumed to be identical to row processing.

`PublishRequest` identifies the immutable source data, pinned schema, validator
semantics, encoding, and allowed user metadata. Objects Publish validates the actual
referenced data and only then publishes a new typed snapshot/envelope through the
artifact layer. The typed snapshot pins exact data/schema identities and validation
semantics; it is not a mutable valid flag or a metadata label. Unchanged payload
bytes remain reusable in CAS. A prior caller-supplied ValidationReport alone is not
authority to bypass validation or substitute different data.

Ordinary artifact publication must reject reserved validated-type claims; raw
metadata saying a type name is never accepted as proof. Accepted claims must be
bound to exact immutable content/schema and issued through the trusted typed-publication
coordinator. Artifacts remains generic and does not interpret column types. The
precise capability/attestation boundary, trusted validator identity, and durable
validation-evidence format must be designed before implementation; this proposal
does not claim the current artifact API already enforces them. Cross-record failures
must distinguish committed data, accepted validation, typed publication, and recorded
lineage, with idempotent recovery; incomplete steps must not advertise complete typed
publication/provenance or pretend committed blobs were rolled back.

A schema change creates a new schema artifact. Lineage records schema revisions,
change descriptions, and explicit identity mappings; existing data remains pinned
to its old schema. Migration produces and validates new data and records derivation
from the exact old data/schema revisions. User-defined schemas can appear as user-facing
Types, while validation evidence and indexes are internal supporting records.

**Content queries.** `QueryRequest` names an adapter/language, explicit immutable
input snapshots with pinned history revisions, parameters, execution limits, result
mode, and required detailed-lineage coverage. `ResultStream` is a closeable, bounded
stream of adapter-described result batches plus terminal status and provenance
coverage/evidence references; row counts or an early close do not imply a complete
result. Cancellation propagates to supported execution steps; arbitrary blocked
engines are not assumed forcibly interruptible. Concrete batch/encoding interfaces
are deferred until supported physical formats are chosen.

Capabilities describes supported languages/options, data models, constraints,
streaming limits, and lineage coverage by operator. SQL/table adapters and JSON
adapters have different capabilities; no common query semantics or arbitrary
SQL/JSON support is promised. Queries are read-only over exact snapshots by default.
Transient results need not be stored. Saving materialized results runs through typed
validation/publication and records provenance; it never mutates input artifacts.
A saved view stores its definition, adapter semantics/options, and pinned input
revisions rather than pretending to contain materialized rows. Floating latest
references are resolved to exact revisions before each execution and recorded with
that execution; the persisted provenance is never just a floating name.

Content queries are distinct from catalog metadata discovery. The catalog finds
schemas/typed artifacts using supported indexed metadata and exact schema revision
selectors; it does not execute SQL or JSON expressions against data. Derived adapter
caches/indexes are keyed by immutable inputs/schema IDs plus adapter version/options
and relevant validation semantics. They are replaceable accelerators, not authoritative
snapshots or provenance. Query and validation operations propagate context and report
their own semantic outcomes through shared observability.

### Required row and column provenance

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

### Lineage

The lineage methods belong to a proposed store interface; `Record` is also
the package's proposed record type. It contains a caller-supplied stable `ID` and
exactly one payload: `Activity`, `Relationship`, or `Revision`. This tagged record
model gives all record kinds the same persistence, lookup, retry, and query rules
without requiring a separate CRUD API for each. Referenced proposed types are:

- `Activity`: caller-defined `Kind`, optional `Inputs`/`Outputs` as lists of Ref,
  optional actor/external-source references, and caller metadata. Ref identifies
  an `artifacts.ID`, a lineage revision ID, or a version-qualified Objects
  sub-object selector as described above. Import may have no inputs;
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
  stable snapshot as defined by the query contract below. Reference-role filters
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

### Shared observability

Observability depends only on Go's standard library. Each layer starts a scope,
passes its derived context to dependencies, and reports its own outcomes. For
example, storage publication, CAS reuse, artifact snapshot completion, and lineage
record acceptance are different semantic counters. Child bytes are not rolled up
into a parent's logical totals. A caller attaches configuration once; concurrent
children inherit it and retain parent-child links. Cancellation remains cooperative,
with commit boundaries decided by the owning layer; it is not a cross-layer rollback
transaction. The existing process-local metrics, callback, and reader-lifetime
contracts below apply to the shared facility, not to unimplemented domain behavior.

## Proposed query contracts

These are API/query design decisions, not new implemented behavior or a persistence
schema. The type descriptions below specify filter meanings and response bounds;
concrete Go record layouts, database tables, and backend constructors are outside
this pass. All query operations accept context first and are scoped to the logical
store instance; cursors cannot cross stores or query kinds.

### Digest access and artifact discovery

CAS remains digest-only: Open/Exists require a known ID. No filename, arbitrary
metadata search, inventory enumeration, or domain query is added to storage/CAS.
Artifacts Get/OpenFile operate on a known immutable snapshot; discovery belongs
to the artifact catalog owned by the artifacts layer.

- `FileQuery` supports an optional normalized directory prefix, Limit, and Cursor.
  Prefix matches whole path segments (for example `dir/`, not `directory/`).
  `FilePage` contains bounded manifest entries and NextCursor, ordered by ascending
  UTF-8 path bytes. It lists entries within one known snapshot, never payload
  bytes. The cursor binds the snapshot ID and prefix. Get still returns a bounded
  complete manifest; ListFiles avoids requiring callers to materialize that list.
- `CatalogQuery` supports an optional exact artifact ID, exact accepted schema
  revision ID for typed publications, inclusive minimum/maximum
  file count and total logical file bytes, and a bounded list of metadata equality
  filters. Predicates are ANDed; no free-form expression language is accepted.
  Logical bytes sum entry sizes, not unique CAS bytes or disk allocation.
- Indexed metadata is limited to up to 16 configured top-level manifest metadata
  keys per store. Values must be scalar strings, booleans, or integers within the
  signed 64-bit range; equality is typed, string comparison is case-sensitive,
  and missing never equals a value. No coercion, null/array/object matching,
  nested JSON paths, regex, full-text, or arbitrary JSON queries are promised.
  Configured indexed strings are bounded to 256 UTF-8 bytes. Publication rejects
  unsupported values for configured keys; other metadata remains opaque and
  unindexed. A filter on an unconfigured key is an error, not an empty result.
- `CatalogPage` returns artifact IDs with file-count/logical-byte summaries and
  NextCursor, ordered by artifact ID bytes. Fetch the manifest through Get when
  needed; search does not eagerly load manifests, payloads, or lineage graphs.

Indexed-key configuration is explicit store policy, fixed for an index generation.
Changing it requires validating existing publications and building a replacement
index. Invalid existing values block that change; queries must not silently omit
records. This policy does not introduce metadata-schema APIs.

### Direct lineage queries and traversal

`Query` combines optional record kind, caller-defined activity/relationship kind,
referenced Ref, reference role, establishing activity ID, Limit, and Cursor. Roles
are typed choices: activity input/output, relationship from/to, revision snapshot,
or revision parent. An omitted role means any reference position; a role requires
a Ref, and incompatible kind/role combinations are validation errors. Actor and
external references remain available on records, not generic metadata search.
List returns immutable records in ascending record-ID byte order.

Producer lookup is List with activity-output Ref; consumer lookup uses activity-input
Ref. Relationship endpoints distinguish source from target. Revision-snapshot finds
histories for a snapshot; revision-parent finds direct children. Get retrieves a
known revision and its parents, or a referenced activity. Establishing-activity
filter finds revision/relationship records attributed to an activity. These are
filters over List, not separate APIs. A shared output does not merge activities
or imply provenance. Imports and annotations need not have both inputs and outputs.

`TraversalQuery` selects one root Ref, an explicit relationship-kind set, direction,
MaxDepth, MaxNodes, MaxResults, Limit, and Cursor. `TraversalPage` returns discovered
Refs with depth, bounded edge records (including establishing activity references
when present), NextCursor, and completion/truncation reason. Traversal follows
explicit relationships, revision-parent links, and evidenced sub-object mappings only; it does not manufacture
an all-inputs-to-all-outputs causal graph from activities.

- Default relationship kind is derived-from; revision-of must be requested
  explicitly. For derived-from, outgoing follows From (derived) to To (source),
  and incoming finds downstream derivatives. For revision-of, outgoing follows
  child revision to parents and incoming follows parent to children. Both is an
  explicit direction option. Default direction is outgoing.
- Use breadth-first traversal, with deterministic edge order by kind and immutable
  edge identity (relationship record ID, or child/parent revision-ID pair).
  Emit each edge once and expand each typed Ref once at its shortest depth. Cycles
  in generic relationships therefore terminate; revision identities are not
  collapsed merely because they point at the same artifact snapshot.
- Defaults are depth 4, 1,000 visited nodes, and 2,000 edge results; hard maxima
  are depth 32, 10,000 nodes, and 20,000 edges. Root is depth zero and counts toward
  the node budget. Each page is also bounded by Limit. Counts and visited state
  carry across pages; requesting another page cannot reset traversal budgets.
- Reaching a query-wide bound returns an explicit truncated/bound reason, never
  an unqualified complete graph. NextCursor continues only the remaining work
  within the same budgets. Each page contains at most Limit newly discovered Refs
  and at most Limit edge records. When that work is exhausted, NextCursor is empty;
  a larger-budget request starts a new traversal. Depth-bound traversal may
  conservatively report a boundary without expanding it to prove more edges exist.

### Detailed provenance queries

Lineage List/Traverse accept the same Objects-qualified row/column selectors as
other Refs, with typed dependency-role filters for value derivation versus predicate/
control dependencies. An exact schema revision selector is also an explicit typed
catalog filter for accepted typed publications, not a search over arbitrary claimed
metadata. Until the validated-claim enforcement boundary exists, this is proposed
behavior and cannot be inferred from an untrusted type label.

Traversals expand explicitly persisted mapping evidence through registered selector/
mapping handlers, alongside existing relationship/revision edges; they never infer
row ancestry from activity input/output lists. Existing direction, cycle detection,
snapshot cursor, and depth/node/result bounds apply across histories and mapping
pages. Deduplication uses the full version-qualified sub-object identity, not just a
row offset, schema name, or artifact digest. Additional decoding/work budgets must
bound compact-mapping expansion, so one compressed edge cannot imply unbounded work.
Results retain dependency roles and report evidence coverage separately from query
truncation: complete evidence can still yield a deliberately bounded answer.

Mapping artifacts/indexes are Internal by default for discovery and diagnostic
observation. A user explicitly requesting row/column provenance receives the semantic
answers for the selected user-visible objects without needing IncludeInternal merely
to resolve their supporting evidence. This is not promotion of support records into
ordinary browsing: raw mapping documents remain hidden unless diagnostically requested.
Internal lookup spans stay internal, while the user-facing query reports its logical
answers, inclusive duration, and failures. Reference resolution/validation remains
complete; visibility filters cannot turn missing evidence into a completeness claim.

### Default discovery classification

The following catalog/lineage policy is proposed, not implemented browsing code.
Record classification is explicit User or Internal, assigned by the owning
publication/record orchestration and retained authoritatively for index rebuilds.
It is not inferred from filenames, hashes, or arbitrary caller metadata. Ordinary
artifacts/activities default to User; supporting manifests, indexes, and validation
records use Internal. A manifest is the representation of its artifact, not a
second user-facing artifact. User-defined schema artifacts can remain User and
appear in a dedicated Types view; schema-related content is not automatically hidden.

CatalogQuery, lineage Query, and TraversalQuery gain an IncludeInternal boolean,
false by default. Filtering happens before visible result counts and pagination;
diagnostic queries explicitly opt into both categories. Internal indexes retain
complete facts for lookup, validation, reference resolution, and provenance. Known-ID
Get/Open operations continue resolving internal references; reporting filters are
not authorization, deletion, or validation bypass. File listing returns the actual
entries of the selected artifact, not separate support files used to store it.

Traversal resolves dependencies internally and retains original edge/record
identities: hidden nodes do not become invented direct edges between visible nodes.
It excludes raw internal support records/Refs from default emitted results; requested
semantic provenance answers are resolved as described above. Work bounds
still cover all visited nodes/edges so filtering cannot evade resource limits.
Visible result counts exclude hidden work; diagnostic reporting can expose it.
An incomplete visible view must not be presented as the entire unfiltered graph.

IncludeInternal is part of the cursor's bound query and cannot change on resume.
Classification changes cannot silently rewrite a cursor snapshot. The initial
proposal has no reclassification API: publication/record retries retain their
original classification and conflicting classification reuse errors. Derived index
rebuilds must preserve classification. Exact persistence fields remain backend
schema work, not additions to implemented artifact/lineage code.

### Pagination and concurrent updates

Proposed collection defaults: Limit 100, maximum 1,000; invalid/negative bounds
are errors and zero selects the default. File/catalog/record lists and traversals
all return opaque cursors. Cursors bind store, query kind, normalized filters,
ordering, index generation, and a fixed snapshot watermark. Changing filters or
bounds while resuming is an error. Increasing page size is also treated as a new
query, keeping resumed work unambiguous.

Choose **snapshot pagination**, not a moving best-effort feed. A fresh catalog or
lineage query captures the authoritative store's committed watermark at admission
and waits until its index can serve that watermark. Later commits do not enter
subsequent pages. Ordering and membership remain fixed, so concurrent appends
cannot create duplicates or skipped members within a cursor session. File-list
snapshots are the immutable manifest ID and need no live-index watermark.

Cursor sessions expire 15 minutes after the first page (not sliding on access).
Expired, invalid, or unavailable-generation cursors return explicit errors; callers
restart rather than silently switching snapshots. Index replacement retains active
generations for this interval or explicitly invalidates sessions on failure.
Traversal may retain a bounded server-side frontier/visited set, addressed by the
opaque cursor; this is not a promise of stateless tokens. Page results are replayable
for the same cursor while its snapshot is retained. Session allocation is bounded;
resource exhaustion is an explicit error, not an unbounded graph load.

### Authoritative facts, indexes, and visibility

CAS blobs/manifests and lineage records are immutable facts. Artifact publication
membership also needs an authoritative durable ledger: arbitrary CAS bytes are
not automatically a published/searchable artifact. Artifact Publish first verifies
and commits the manifest, then idempotently registers its ID in that ledger.
An identical snapshot has one catalog membership regardless of repeated Publish.
Successful Publish requires both steps; payload availability alone is insufficient.

The lineage layer owns durable, append-only record persistence with atomic unique-ID
validation and ordered commit watermarks; conflicting caller-ID reuse fails while
identical retries return the existing record. Its indexes are rebuildable views,
not the sole authoritative copy. The artifact layer owns its catalog and membership
ledger; lineage owns record/reference/adjacency indexes. Neither responsibility
belongs in blob storage. The specific database/log engine and on-disk encoding
remain genuinely unsettled, but must satisfy these contracts.

The proposed update path is a durable commit followed by ordered index projection;
index lag is allowed internally, not as silently stale fresh-query results. A new
query admitted after a successful Publish/Record must include that commit if it
matches. It waits for its captured watermark, respecting context deadlines; an
unavailable index produces an error. There is no implicit eventual-consistency
mode. Known-ID artifact/record reads use authoritative data and do not require a
healthy search index. An old cursor intentionally does not see later publications.

If content or records commit but a later registration/cleanup step fails, return
the stable ID and an error distinguishing what committed; never imply rollback.
The caller retries the same content/record identity to complete registration.
A manifest committed before failed membership registration can be read by known
ID but is not catalog-visible until registration succeeds. Projection failures
after authoritative commit retain durable pending work and retry; publication need
not wait for projection, but fresh queries cannot pass the visibility barrier until
projection catches up. A failed durable commit must not advance the watermark.

Each owning layer replays its authoritative ledger into a new index generation,
checks it at a captured watermark, catches up concurrent commits, then atomically
switches the active generation. Rebuild failure keeps the previous generation;
unavailability is reported if no valid generation remains. Index rebuild never
creates new activities, artifact identities, or revisions. Operational scheduling,
engine-specific transactions, cursor storage, and rebuild tooling are backend
choices to settle before implementation, not extra public schema APIs here.

### Cross-layer composition and observation

A typical query searches the artifact catalog with typed predicates, uses returned
IDs in bounded lineage List/Traverse queries, then fetches selected manifests/files
through artifacts and CAS. It pages and fetches on demand rather than loading the
entire catalog or giving storage knowledge of artifacts. Stores must share the
intended logical scope; independent layers have independent watermarks, so this
composition does not promise a globally transactional cross-layer snapshot.

Every query propagates context to child work and reports a layer-specific scope.
Observe scanned/returned records, traversal visits, and index waits as explicit
operation counters; do not treat those counters as persistent inventory. Check
cancellation between pages, expansion steps, and backend calls where supported.
Do not return a canceled/failed page as complete: discard that incomplete page,
retain any earlier completed pages, and let callers retry the same valid cursor.
Cancellation does not undo committed records or force arbitrary blocked I/O to stop.

## Attach observation once

```go
package example

import (
    "context"
    "io"
    "strings"
    "time"

    "github.com/craggy-io/artifact-services/observability"
    "github.com/craggy-io/artifact-services/storage"
)

func RoundTrip(root string, output io.Writer) error {
    backend, err := storage.NewFilesystem(root)
    if err != nil { return err }
    var metrics observability.Collector
    ctx := observability.With(context.Background(), observability.Options{
        Collector: &metrics,
        ProgressInterval: 100 * time.Millisecond,
        Observer: func(event observability.Event) {
            // Send to a concurrency-safe logger/UI queue if desired.
            // event.Final ends a scope; bytes alone do not prove publication.
        },
    })
    if err := backend.Put(ctx, "abcd0123", strings.NewReader("hello")); err != nil {
        return err
    }
    reader, err := backend.Open(ctx, "abcd0123")
    if err != nil { return err }
    _, readErr := io.Copy(output, reader)
    closeErr := reader.Close()
    _ = metrics.Snapshot(observability.SnapshotOptions{})
    if readErr != nil { return readErr }
    return closeErr
}
```

Each implemented layer starts its own scope and propagates the returned context.
For example, an application can group real storage calls without inventing CAS:

```go
ctx, operation := observability.Start(ctx,
    observability.Scope{Layer: "application", Operation: "upload"}, "request-id")
err := backend.Put(ctx, "abcd0123", source)
if err == nil { operation.Count("completed_uploads", 1) }
operation.End(err)
```

The storage span has the application span's ID as its ParentID. Derived contexts
(including cancellation/deadline contexts) inherit configuration and linkage.
Concurrent children started with the same parent context are siblings with unique
IDs. Parents do not automatically aggregate/end children or infer their semantic
outcomes. Future layers can report counters such as `dedup` explicitly; no CAS or
artifact functionality is supplied by observation itself.

## Observation classification (implemented)

Direct storage operations default to `observability.User`. Mark support work with
`observability.WithInternal(ctx)` before starting it; all descendant spans inherit
`observability.Internal`, even across `With`, cancellation contexts, and concurrent
children. Classification is sticky: no API resets an internal descendant to User.
Start user-facing siblings from the original user context. This deliberate scoping
rule preserves trace/parent IDs without deriving meaning from storage keys.

```go
// ctx already carries the collector/observer configured above.
internalCtx := observability.WithInternal(ctx)
// Any storage calls made with internalCtx now report internal work.
_ = internalCtx
visible := metrics.Snapshot(observability.SnapshotOptions{})
diagnostic := metrics.Snapshot(observability.SnapshotOptions{IncludeInternal: true})
_, _ = visible, diagnostic
```

Observer events exclude Internal by default. Set `Options.IncludeInternal: true`
when configuring observation to emit future diagnostic events. Every event carries
Category. Enabling it later does not replay previously filtered events or fabricate
missing parent events; parent IDs still describe the original trace.

An attached collector always retains separate User/Internal aggregate buckets,
including work whose events were filtered. `Snapshot(SnapshotOptions{})` excludes
Internal; `Snapshot(SnapshotOptions{IncludeInternal: true})` sums both buckets per
scope. The latter is a diagnostic work view, not a unique-user-payload view.
No data predating attachment, or from contexts with collection disabled, can be
recovered. Buckets store bounded scope/counter labels, not event histories or keys;
collector lifetime/reset-by-replacement is controlled by the application.

Spans report their own work; child bytes/counters are never automatically summed
into user parents. Orchestration must count logical user payload/artifact outcomes
on the appropriate user scope and report support bytes on internal scopes, rather
than manually copying hidden bytes into a parent. Per-layer stream-byte metrics
are process work, not physical disk measurements; summing layers is not a unique
payload total. Parent elapsed time and metric Duration remain **inclusive** of
internal work. Parent errors remain visible even when caused by internal work.
Classification filters reporting only; it does not alter I/O, cancellation,
validation, publication, or error propagation. The matching discovery policy above
remains a proposal because catalog/lineage implementations do not exist yet.

## Observation contract

- `With` attaches options only to a context subtree; empty options disable
  observation there. Without observer/collector, `Start` returns the original
  context and a nil span without allocating a scope. All nil-span methods are
  no-ops. There is no global collector. Use non-nil contexts throughout.
- Observer and collector are independently optional. Collectors are zero-value
  usable and concurrency-safe; do not copy after use. Selected snapshots are independent
  copies keyed by `Scope{Layer, Operation}`, with started/completed/errors, bytes,
  summed inclusive completed duration, and named semantic counters. Active is started
  minus completed. Use bounded scope/counter labels; keys are not retained.
- Events carry ID/ParentID, scope/key, Category, cumulative bytes/counters, inclusive elapsed time,
  optional total, error, and Final. `Options.Total` may supply a size hint by
  scope/key; absent, negative, or unknown values become -1. Totals are not
  validated or converted to a fabricated percentage.
- Byte updates are throttled per span (default 100ms); selected start, semantic counts,
  recorded errors, and final updates always emit. No timer emits during idle I/O.
  End is idempotent; reporting after End does nothing. Span methods require
  serial calls; independent child spans may run concurrently.
- Callbacks run synchronously and may run concurrently across spans. They must
  not panic or reenter the same span/reader. They may inspect snapshots or start
  other spans; no collector lock is held during callbacks. Slow callbacks delay
  work and contribute to lifetime duration.
- Storage reports scopes `storage/put`, `storage/open`, and `storage/exists`.
  Put increments `published` only after actual publication, even if cleanup
  fails. Open increments `eof` once, `closed` once, and `early_closed` when closed
  without observing EOF. Errors count terminal failing operations, not error
  values. Bytes count consumed/transferred stream bytes, including `(n, err)`.
- Open spans include opening and the returned reader's lifetime. EOF/read errors
  produce updates; only Close finalizes, preserving close errors. Final errors
  join read/close errors; Close returns only its original close error. Repeated
  Close returns the cached error without closing/counting again; reads after
  Close return `io.ErrClosedPipe`. Individual readers require serial use.
  Abandoned readers remain active. Reading exactly a known size without asking
  for EOF still counts as early close.
- Metrics cover only explicit operations observed in this process. Child bytes
  are not rolled into parents. Layer totals must not be summed as unique bytes,
  logical dedup savings, disk I/O/allocation, or persistent/global inventory.

## Storage and cancellation contract

`Store` is replaceable and safe for concurrent operations. It owns physical layout
and publication, not hashing, identity validation, deduplication, or metadata.

- Keys are 2–128 ASCII lowercase hexadecimal characters. Other characters,
  separators, uppercase, and traversal return `ErrInvalidKey`. Layout is one file
  per blob at `<root>/<first-two>/<key>`; paths stay internal except diagnostics.
- Put never overwrites. Concurrent same-key writers have one publisher; losing
  completed writers return `ErrAlreadyExists`. A duplicate may consume its source.
- Every Put failure is wrapped in `*storage.PutError`, whose Published field
  describes this call. Use `errors.As` to inspect it and `errors.Is`/`errors.As`
  for retained source/OS errors. Nil means published. Cleanup failure after
  publication has Published=true; pre-publication failure has Published=false,
  even if an input error contains another operation's published PutError.
- Open returns `ErrNotFound` for missing blobs; Exists returns false, nil. Other
  errors propagate; non-regular entries are errors. Callers close returned readers;
  storage never closes supplied source readers.
- Writes stream into a temporary file, sync and close it, then atomically create
  a hard link without replacement. Readers see absence or complete data.
  Temporary cleanup runs on success/failure; cleanup errors retain other causes.
  Empty shards can remain. An empty deleted root is recreated by a subsequent Put.
- All operations check cancellation before filesystem work. Put also checks
  around stream reads, before writes/sync, and immediately before publication.
  Open retains its context in the reader and checks between reads. Cancellation
  cannot interrupt an arbitrary blocked reader, filesystem call, or Sync. A read
  that returns bytes as cancellation arrives still counts those bytes.
- Cancellation observed before publication prevents publication and triggers
  cleanup. Cancellation racing the final check/hard-link operation may still
  publish. Once published, it is not rolled back: success (or Published=true on
  cleanup failure) remains authoritative even if the context is now canceled.
  Close always releases resources despite cancellation; cancellation alone after
  the last read does not manufacture a Close error.

The root must be private/trusted; external edits, symlinks, and junctions are
unsupported. No deletion/lifecycle API exists. A Docker-mounted root needs no
special code, but its filesystem must support atomic non-replacing hard links.
Unsupported filesystems return errors; there is no copy/overwrite fallback.
Directory entries are not synced, so crash/power-loss durability is not guaranteed.
Crashes can leave temporary files; automatic crash recovery is not implemented.

## Development and verification

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

### Opt-in 1 GiB Windows trial

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
