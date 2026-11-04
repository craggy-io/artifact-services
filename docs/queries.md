# Query contracts

[Project](../README.md) · [Documentation index](README.md) · [Walkthrough](walkthrough.md)

These are API/query design decisions, not new implemented behavior or a persistence
schema. The type descriptions below specify filter meanings and response bounds;
concrete Go record layouts, database tables, and backend constructors are outside
this pass. All query operations accept context first and are scoped to the logical
store instance; cursors cannot cross stores or query kinds.

## Digest access and artifact discovery

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

## Direct lineage queries and traversal

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

## Detailed provenance queries

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

## Default discovery classification

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
semantic provenance answers are resolved as described in [detailed provenance](lineage.md#semantics-errors-and-limits). Work bounds
still cover all visited nodes/edges so filtering cannot evade resource limits.
Visible result counts exclude hidden work; diagnostic reporting can expose it.
An incomplete visible view must not be presented as the entire unfiltered graph.

IncludeInternal is part of the cursor's bound query and cannot change on resume.
Classification changes cannot silently rewrite a cursor snapshot. The initial
proposal has no reclassification API: publication/record retries retain their
original classification and conflicting classification reuse errors. Derived index
rebuilds must preserve classification. Exact persistence fields remain backend
schema work, not additions to implemented artifact/lineage code.

## Pagination and concurrent updates

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

## Authoritative facts, indexes, and visibility

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

## Cross-layer composition and observation

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
