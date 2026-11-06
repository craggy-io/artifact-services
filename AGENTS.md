# Artifact Services: coding-agent integration guide

Use this guide when editing this repository or integrating its Go module,
`github.com/craggy-io/artifact-services`, into a consumer. Repository development
rules below apply to work here. Consumers should explicitly provide this guide to
their coding agent; installing a dependency does not imply its AGENTS.md is read.
Start with the [walkthrough](docs/walkthrough.md) and
[executable integration examples](integration/system_test.go).

## Choose the owning layer

| Need | Layer and contract |
| --- | --- |
| Opaque keyed blobs, streaming I/O, atomic non-replacing publication | [Storage](docs/storage.md) |
| SHA-256 identity, verified reuse, digest-checked reads | [CAS](docs/cas.md) |
| Immutable file/collection snapshots and metadata discovery | [Artifacts](docs/artifacts.md) |
| Runtime table/JSON schemas, typed validation and content queries | [Objects](docs/objects.md) |
| Generic activities, independent revisions and explicit provenance mappings | [Lineage](docs/lineage.md) |
| Typed statements, current views, keyed uniqueness and writer authority | [Assertions](docs/assertions.md) |
| Context scopes, progress, semantic counters and visibility | [Observability](docs/observability.md) |
| Wiring, runs, typed sugar, tree I/O, spooling, GC, journal watch, test kit, read-only SQL | [Adapters](docs/adapters.md) |

Objects, lineage and assertions sit over artifacts; artifacts uses CAS, which uses
storage. Adapters compose public APIs only and hold no privilege. Shared `provenance` types carry neutral references and evidence. Keep
schema/query semantics out of storage and CAS. Catalog search finds snapshots;
Objects queries their content. Lineage records assertions, not execution or proof
of causal truth.

## Construct and close services

`adapters/stack.Open` does all of this under one root; prefer it unless you need only some
layers. Otherwise construct what the application needs, in dependency order:

1. `storage.NewFilesystem(blobRoot)`.
2. `cas.New(blobs, cas.Options{TempDir: stagingDir})`. A supplied staging directory
   must already exist; empty selects the OS temporary directory.
3. `artifacts.New(content, artifacts.Options{Root: catalogRoot})`.
4. `objects.New(content, snapshots, objects.Options{})` when typed data is needed.
5. `lineage.New(snapshots, lineage.Options{Root: lineageRoot,
   ValidateSelector: typed.ValidateRef})` for histories with detailed selectors.
   Here `typed` is the Objects service. Without a selector validator, detailed
   row/column references are rejected rather than assumed valid.
6. `assertions.New(snapshots, assertions.Options{Root: assertionRoot, Authority: authority})`
   when statements are needed. Keep the `Authority` on a trusted construction path and hand
   out only the credentials callers need. Wire `ValidateSelector`, `ValidateRevision` and
   `ValidateActivity` for the reference forms in use.

Handle each constructor error before using the result. Use distinct storage, catalog,
lineage and assertion roots. Artifact, lineage and assertion journals each take an exclusive
OS file lock on their root: one live service owns each. A dead owner's lock disappears with
it, so reopening needs no recovery step; never delete a `.lease` file. Close methods release
the locks; after stopping work and closing streams, close assertions, then lineage, then
artifacts. Storage, CAS, and Objects have no service Close method. Closing an artifact
service does not close shared CAS or outstanding file readers. Storage's concurrent-writer
contract is distinct from journal root exclusivity. A successful commit has been flushed, so
it survives process death and, as far as the OS and hardware honour a flush, power loss;
`Options.Relaxed` gives that up for speed. See [durability](docs/durability.md).

## Context, streams, and errors

- Pass a non-nil context through every domain operation. Attach optional reporting
  once with `observability.With`; propagate the derived context returned by
  `observability.Start` to child work and end each scope with its outcome.
- User is the default category. `observability.WithInternal` marks supporting work
  and its descendants Internal; default events/statistics omit it. Use the original
  User context for subsequent user-facing work. Diagnostic IncludeInternal settings
  expose internal work; they do not change execution or authorization. Each scope
  owns its counters; summing child bytes is not unique payload size.
- Callers own supplied readers and must close returned storage/CAS/artifact readers
  and Objects result streams, retaining relevant read and close errors. CAS integrity
  is established only at matching EOF. Early Close does not drain or verify content;
  stage bytes before use if the consumer requires verification before processing.
- Cancellation is cooperative. It cannot force arbitrary blocked readers or OS calls
  to stop, and can race publication. Never infer rollback from a canceled context.
- Use `errors.As` for stage information and `errors.Is` for retained causes:
  `storage.PutError.Published`, `cas.PutError.Committed`,
  `artifacts.PublishError.ManifestCommitted`/`Registered`,
  `artifacts.OccurrenceError.Committed`, `lineage.RecordError.Committed`, and
  `assertions.RecordError.Committed`. Objects publication/materialization errors also
  retain stage information; inspect their documented outcomes. A returned ID alone
  is not proof of success. Cleanup can fail after commitment; retain stable IDs and
  retry according to the owning layer's idempotency contract.

## Typed publication and exact histories

Schemas and data are immutable artifacts. A schema change creates another artifact;
existing data keeps its pinned schema. Use Objects validation/publication to bind
actual data, schema, encoding, and validator semantics. Ordinary artifact metadata
cannot manufacture validated type status; do not call the internal typed-claim seam
from consumers or substitute a caller-supplied validation report for validation.

Pin exact artifact and history revision identities in queries and evidence. Equal
bytes, row offsets, or display names do not establish ancestry. Record output
revisions before detailed evidence referencing them; ordinary artifact-only
activities can precede their revisions. Keep revision parent links within the
intended history and derivation links across histories explicit.

`SaveView` stores a definition and exact inputs, not computed rows. `Materialize`
publishes validated table results and mapping evidence; the caller coordinates
separate lineage records. This is not one cross-layer rollback transaction. Follow
the [composite protocol](docs/durability.md#composite-operations) for publication and
recording order, with stable IDs at each step, or use [`adapters/runs`](docs/adapters.md#runs).

`artifacts.ID` is content-derived: publishing equal bytes twice gives one ID. To describe two
publications separately, record two occurrences, and reference `Occurrence` to stay with one,
`Artifact` for the snapshot, or `Digest` to follow content.

## Queries and limits

Call `typed.Capabilities(ctx)` on the `objects.Service` instance and inspect its
configured limits before constructing queries.
The supported `sql-v1` subset includes SELECT, equality INNER JOIN, comparison/AND
filters, arithmetic, GROUP BY, COUNT, and SUM. Unsupported SQL is rejected.
`json-pointer-v1` is JSON Pointer, not JSONPath or arbitrary JSON queries; it reports
document dependencies rather than table row/column mappings. Coverage describes
supported emitted-result dependencies, not counterfactual explanations. Check
terminal completion and coverage, including early-close outcomes.

Validation and query execution have explicit byte, row, result, join, group, and
dependency bounds. Validation, `Rows` and single-table non-aggregate SQL stream, and those
queries report evaluation and limit errors from `Next`; joins and aggregates are resident
within their limits. Lineage separately bounds evidence and traversal work. Distinguish
evidence coverage from graph truncation. See [Objects](docs/objects.md),
[lineage](docs/lineage.md), and [query contracts](docs/queries.md).

Catalog, lineage and assertion cursors bind filters/bounds to fixed snapshots. Keep the query
unchanged while resuming; later writes do not enter those pages. Cursors expire and
do not survive service restart. Handle expiry, invalid cursors, and resource limits
explicitly. Snapshot consistency is local to each query, not a transaction across
all layers. Internal discovery is opt-in; known-ID access is not an authorization
boundary. Journal projections can be rebuilt from authoritative records. For a lossless feed, keep a
position and read `Changes`; `OnCommit` is a coalesced wake-up, not a delivery log.
`adapters/watch` does both for all three journals.

## Developing and verifying in this repository

- Preserve concurrent edits and reread before patching. Use `rg` for searches.
- Check existing helpers before adding boilerplate. On the second occurrence of
  a pattern, extract or widen a small shared helper at the narrowest useful scope.
- After changing a data model, trace every reader, formatter, serializer, template,
  and downstream consumer; silent field omissions are bugs.
- Fully remove obsolete code and call sites rather than adding deprecation markers,
  compatibility shims, or commented-out remnants. Coordinate cross-team API removal
  with the user when needed.
- Verify fixes by running the affected code and existing tests. If coverage is
  missing, add and run a minimal regression test or reproduction. Report what ran
  and any limits; do not equate an unrun check with a pass.

From this module root, using the Go version specified in [go.mod](go.mod):

```sh
test -z "$(gofmt -l .)"
go test ./integration -count=1
go test ./...
go test -race ./...
go vet ./...
go build ./...
(cd adapters/sqlindex && go test ./...)
```

`TestExportedAPIMatchesGolden` fails on any exported-API change. Regenerate `api.golden.txt`
as described in [stability](docs/stability.md) and review its diff.

Run focused package tests while iterating and relevant broader checks for the
change. In a consumer repository, run that application's integration tests against
its actual service construction, stream ownership, and failure handling; these
module commands do not replace consumer tests. The opt-in large-file trials and
qualification limits live in [verification](docs/verification.md). This guide lists
verification commands, not a claim that a particular checkout has passed them.
