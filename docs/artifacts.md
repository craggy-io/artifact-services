# Artifacts

[Project](../README.md) · [Documentation index](README.md) · [Walkthrough](walkthrough.md)

Proposed API design only; this package is not implemented. Examples are illustrative, not runnable APIs.

## Purpose

Describe immutable single-file or collection snapshots.

## Mental model

A canonical manifest names a snapshot; its entries reuse CAS bytes.

## Responsibilities and boundaries

`artifacts.ID` identifies a canonical, versioned manifest by content. `Manifest`
contains `Entries []Entry` and `Metadata map[string]json.RawMessage`; each Entry
contains a normalized relative `Path`, a `cas.ID` content reference, and `Size
int64`. One entry represents a single-file snapshot; multiple entries represent a
collection. Paths are unique, case-sensitive slash-separated names: absolute
paths, backslashes, empty segments, and dot/traversal segments are rejected.
These are logical paths, not permission to extract directly onto a host filesystem.

Publish validates the manifest, checks each referenced CAS blob for availability,
size and integrity, then publishes the canonical manifest atomically through CAS.
Only after that publication and durable catalog registration (see [query contracts](queries.md)) may
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

## Public interfaces

- `Publish(ctx context.Context, manifest Manifest) (ID, error)`
- `Get(ctx context.Context, id ID) (Manifest, error)`
- `OpenFile(ctx context.Context, id ID, path string) (io.ReadCloser, error)`
- `ListFiles(ctx context.Context, id ID, query FileQuery) (FilePage, error)`
- `Search(ctx context.Context, query CatalogQuery) (CatalogPage, error)`

## Working with other layers

Depends on [CAS](cas.md). [Objects](objects.md) supplies typed meaning and [lineage](lineage.md) tracks histories; neither is encoded by content equality.

## Examples

Illustrative: publish a manifest mapping app.apk to an existing CAS digest, or a collection of extracted files. Reusing one entry does not create a revision. See [walkthrough](walkthrough.md).

## Semantics, errors, and limits

See [artifact discovery](queries.md#digest-access-and-artifact-discovery), [authority and visibility](queries.md#authoritative-facts-indexes-and-visibility), and [default classification](queries.md#default-discovery-classification). These own catalog publication, indexing, cursor, and read-after-publish semantics; payload/manifest bounds and canonicalization requirements above still apply.

## Validation and evidence

No runtime qualification exists for this proposed layer. The contracts here are design requirements, not test results. Open implementation/encoding decisions remain explicit in this guide and the [query contracts](queries.md).
