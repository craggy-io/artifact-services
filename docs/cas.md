# CAS

[Project](../README.md) · [Documentation index](README.md) · [Walkthrough](walkthrough.md)

Proposed API design only; this package is not implemented. Examples are illustrative, not runnable APIs.

## Purpose

Give immutable bytes a verified content identity.

## Mental model

Equal bytes can share a digest without sharing artifact history.

## Responsibilities and boundaries

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

## Public interfaces

- `Put(ctx context.Context, source io.Reader) (PutResult, error)`
- `Open(ctx context.Context, id ID) (io.ReadCloser, error)`
- `Exists(ctx context.Context, id ID) (bool, error)`

## Working with other layers

CAS builds on [storage](storage.md); [artifacts](artifacts.md) references its content. Digest lookup is deliberately separate from [catalog search](queries.md#digest-access-and-artifact-discovery).

## Examples

Illustrative: stream an APK into Put, retain its digest/size, then read through EOF to verify it. Reusing that digest does not infer ancestry. See the [walkthrough](walkthrough.md).

## Semantics, errors, and limits

The integrity, early-close, duplicate-verification, staging, and committed-with-error requirements are specified above. Context/cancellation and semantic observation follow [observability](observability.md); no runtime CAS implementation is claimed.

## Validation and evidence

No runtime qualification exists for this proposed layer. The contracts here are design requirements, not test results. Open implementation/encoding decisions remain explicit in this guide and the [query contracts](queries.md).
