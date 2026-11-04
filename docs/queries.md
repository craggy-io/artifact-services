# Query contracts

[Project](../README.md) · [Documentation index](README.md) · [Walkthrough](walkthrough.md)

These are implemented bounded artifact and lineage query contracts. Their public
Go types and constructors are documented in the [artifact](artifacts.md) and
[lineage](lineage.md) guides. The backend is local, exclusively owned filesystem
journals with synchronous in-process projections, not a distributed database. All query operations accept context first and are scoped to the logical
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
  artifact ID for typed publications, inclusive minimum/maximum
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
The key policy is persisted and immutable for a catalog root. To change it, open
a new catalog root and republish the same immutable manifests under the new policy;
validation can reject incompatible values. Existing IDs remain content identities,
but catalogs/cursors stay scoped to their original roots. No in-place policy-change
or metadata-schema API is exposed.

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
control dependencies. An exact schema artifact selector is also an explicit typed catalog filter for
accepted typed publications. Accepted claims come from the module-private typed
publication capability and durable registration, not arbitrary metadata in CAS.
See [Objects](objects.md) for validation and supported query semantics.

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

Catalog and lineage discovery apply the following implemented policy.
Record classification is explicit User or Internal, assigned by the owning
publication/record orchestration and retained authoritatively for index rebuilds.
It is not inferred from filenames, hashes, or arbitrary caller metadata. Ordinary
artifacts/activities default to User; supporting manifests, indexes, and validation
records use Internal. A manifest is the representation of its artifact, not a
second user-facing artifact. User-defined schema artifacts can remain User and
appear in a dedicated Types view; schema-related content is not automatically hidden. A dedicated Types UI is not
part of this library; callers can discover schema artifacts through catalog metadata.

CatalogQuery, lineage Query, and TraversalQuery have an IncludeInternal boolean,
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
rebuilds must preserve classification. Classification is persisted with each authoritative membership/record; it is
restored on reopen and projection rebuild.

## Pagination and concurrent updates

Collection defaults: Limit 100, maximum 1,000; invalid/negative bounds
are errors and zero selects the default. File/catalog/record lists and traversals
all return opaque cursors. Cursors bind store, query kind, normalized filters,
ordering, index generation, and a fixed captured result snapshot. Each cache instance also isolates cursors
from other stores and query kinds. Changing filters or
bounds while resuming is an error. Increasing page size is also treated as a new
query, keeping resumed work unambiguous.

**Snapshot pagination** uses the committed in-process projection captured under
the same mutex used for registration/record projection. Commits and projection
updates are synchronous, so no asynchronous index wait is required. Later commits do not enter
subsequent pages. Ordering and membership remain fixed, so concurrent appends
cannot create duplicates or skipped members within a cursor session. File-list
snapshots are the immutable manifest ID and need no live-index watermark.

Cursor sessions expire 15 minutes after the first page (not sliding on access).
Expired, invalid, mismatched, or previous-service-instance cursors return errors;
callers restart rather than silently switching views. Snapshots are copied into a
bounded cache and remain replayable across artifact Rebuild within the same live
service. Reopening a service invalidates its old cursors. Each cache allows 128
sessions, 16 MiB per snapshot, and 64 MiB total; exhaustion is an explicit error.
Traversal computes bounded adjacency/results before paging; it does not expose an
unbounded graph or promise stateless cursors. Limits on decoded evidence and
adjacency are separate from result/page limits; see [lineage limits](lineage.md).

## Authoritative facts, indexes, and visibility

CAS blobs/manifests and lineage records are immutable facts. Artifact publication
membership also needs an authoritative durable ledger: arbitrary CAS bytes are
not automatically a published/searchable artifact. Artifact Publish first verifies
and commits the manifest, then idempotently registers its ID in that ledger.
An identical snapshot has one catalog membership regardless of repeated Publish.
Successful Publish requires both steps; payload availability alone is insufficient.

The lineage layer owns append-only record persistence with atomic unique-ID
validation and ordered commit sequence numbers. Identical retries reuse a record;
conflicting reuse fails. Artifact membership and lineage records each use their own
journal root. Files contain versioned canonical JSON envelopes, published by syncing
a temporary file and creating an immutable hard link at the sequence filename.
The shared internal journal is an implementation utility, not a new public layer.
Canonical JSON bounds both input and expanded output to 8 MiB; duplicate members,
excessive nesting/numeric expansion, and malformed persisted envelopes are rejected.

Each journal has an exclusive `.lease` file: only one live service owns a root.
Call Close to release it. A crashed owner's stale lease requires manual removal
only after confirming that no owner remains; automatic crash recovery is not
implemented. Journal roots are trusted directories, like blob roots. The defaults
are 100,000 records, 128 MiB encoded ledger data, and 8 MiB per record; these bound
replay and projection memory. Duplicate IDs, broken sequences, malformed records,
and incompatible stored policy are errors on reopen. Directory-entry durability,
power-loss recovery, and disk-full/sync fault behavior are not qualified.

Commit and projection update are synchronous under the owning layer's lock.
Successful Publish/Record is visible to subsequent fresh matching queries; an old
cursor deliberately excludes later commits. Query cancellation returns errors and
never labels an incomplete page complete. Known-ID artifact reads can resolve a
manifest directly from CAS even if registration failed; authoritative Category and
validated-claim lookup require accepted registration. Lineage reference validation
requires registered artifacts and existing exact revisions. Public Get does not
turn arbitrary reserved metadata into accepted validation evidence.

If immutable data commits and a subsequent registration/cleanup step fails, the
stable ID and typed stage/outcome error indicate what committed; nothing pretends
to roll back. Retry the same identity to finish registration. Artifact PublishError
distinguishes ManifestCommitted from Registered; lineage RecordError reports its
Committed state. Classified retries must preserve the original category.

Artifact Rebuild validates registrations against their immutable manifests and
atomically replaces its catalog projection after catching up concurrent commits.
Failure leaves the previous projection intact. Lineage replays its authoritative
records when opening and constructs bounded query adjacency from those records
and immutable evidence. Rebuilding projections never creates new logical identities
or activities. There is no async queue to lose: this backend deliberately chooses
synchronous projection and explicit errors over an eventually consistent search
mode. Distributed indexing, automatic compaction, online policy migration, and
crash lease recovery are not supplied.

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
