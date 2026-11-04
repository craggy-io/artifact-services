# Objects

[Project](../README.md) · [Documentation index](README.md) · [Walkthrough](walkthrough.md)

Proposed API design only; this package is not implemented. Examples are illustrative, not runnable APIs.

## Purpose

Attach runtime-defined types and validated content-query semantics to immutable snapshots.

## Mental model

Schema and data are pinned immutable artifacts; a type label alone is not validation.

## Responsibilities and boundaries

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

## Public interfaces

- `DefineSchema(ctx context.Context, definition SchemaDefinition) (artifacts.ID, error)`
- `GetSchema(ctx context.Context, id artifacts.ID) (SchemaDefinition, error)`
- `Validate(ctx context.Context, request ValidationRequest) (ValidationReport, error)`
- `Publish(ctx context.Context, request PublishRequest) (artifacts.ID, error)`
- `Query(ctx context.Context, request QueryRequest) (ResultStream, error)`
- `Capabilities(ctx context.Context) (Capabilities, error)`

## Working with other layers

Objects and [lineage](lineage.md) are peers over [artifacts](artifacts.md). A coordinator composes publication and provenance without circular package dependencies. [Detailed row/column provenance](lineage.md#semantics-errors-and-limits) is a required shared contract.

## Examples

The [walkthrough](walkthrough.md) follows schema evolution, separate table/view histories, a join, and saved versus materialized results. These are illustrative operations, not executable adapter code.

## Semantics, errors, and limits

Runtime schema, enforcement, query-stream, capability and validation limits are specified above. Required detailed evidence and completeness rules live in [lineage](lineage.md#semantics-errors-and-limits); bounded querying and internal support-record treatment live in [query contracts](queries.md#detailed-provenance-queries).

## Validation and evidence

No runtime qualification exists for this proposed layer. The contracts here are design requirements, not test results. Open implementation/encoding decisions remain explicit in this guide and the [query contracts](queries.md).
