# Assertions

[Project](../README.md) · [Documentation index](README.md) · [Durability](durability.md)

Available Go API. Assertions use their own exclusive journal root, as artifacts and
lineage do. Kind names and what any assertion means belong to the consumer.

## Purpose

Record typed statements about stored things, and answer "what is currently said about
this" without the caller reimplementing supersession rules.

## Features

- Register kinds at runtime by name, version and JSON shape. A kind is an immutable, auditable record.
- Bind a validated value to a subject, with evidence references, an optional key, and the recording activity and actor.
- Supersede or retract: "current" is derived by the layer, and history stays readable.
- Enforce one current assertion per (kind, key) at commit, under concurrency, with a conflict error.
- Restrict a kind to named writers through an unforgeable capability.
- Query by subject, kind, key or evidence, paged and bounded, with `Sequence`, `OnCommit` and `Changes`.

## Mental model

A kind declares a shape. An assertion is one immutable journal record. Replacing or
retracting one ends it, recording who ended it and at which sequence; nothing is
rewritten. An assertion is current until something ends it.

Which reference form is the subject decides what an assertion follows:

| Subject | Applies to |
| --- | --- |
| `Occurrence` | That one publication, and never another occurrence of equal bytes |
| `Artifact` | That snapshot, wherever it is named |
| `Digest` | The content, across every occurrence (read with `Query.IncludeContent`) |

## Responsibilities and boundaries

The layer validates values against the kind's shape, resolves references at record
time, enforces writers, namespaces and keyed uniqueness, and stamps each record with
its sequence. It does not know what a kind means or what an identity is. Artifacts
hold content; assertions never do. Lineage records what happened, assertions record
what is claimed, and neither needs the other.

## Public interfaces

- `New(store artifacts.Store, options Options) (*Service, error)`, `NewAuthority() *Authority`
- `(*Authority).Credential(identity string) Writer`
- `Register(ctx, Kind, Writer) (KindID, error)`, `GetKind(ctx, KindID) (Kind, error)`
- `Record(ctx, Request) (ID, error)`, `Retract(ctx, Retraction) error`
- `Get(ctx, ID) (Assertion, error)`, `History(ctx, ID) ([]Assertion, error)`
- `Current(ctx, KindID, key string) (Assertion, error)`
- `List(ctx, Query) (Page, error)`
- `Sequence(ctx) (uint64, error)`, `OnCommit(func(uint64)) (stop func())`,
  `Changes(ctx, provenance.ChangeQuery) (provenance.ChangePage, error)`
- `ValidateAssertion(ctx, id string) error`, `Close() error`

`Options` holds `Root`, `Authority`, `ReservedNamespaces`, `MaxRecords`, `MaxBytes`,
`Relaxed`, and the validators `ValidateSelector`, `ValidateRevision` and
`ValidateActivity`. A reference form with no resolver is rejected, not assumed valid.

## Working with other layers

Subjects and evidence use the [`provenance.Ref` grammar](lineage.md#reference-forms).
Pass `objects.Service.ValidateRef` as `ValidateSelector` and the lineage service's
`ValidateRevision` and `ValidateActivity` for the matching forms. Pass
`assertions.Service.ValidateAssertion` as lineage's `ValidateAssertion` so lineage can
cite assertions. Construction order is resolved for you by
[`adapters/stack`](adapters.md#stack). Go consumers usually want
[`adapters/typed`](adapters.md#typed) rather than raw JSON.

## Examples

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/craggy-io/artifact-services/adapters/stack"
	"github.com/craggy-io/artifact-services/assertions"
	"github.com/craggy-io/artifact-services/objects"
	"github.com/craggy-io/artifact-services/provenance"
)

func main() {
	root, err := os.MkdirTemp("", "assertions-example-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(root)
	ctx := context.Background()

	// Only this construction path holds the authority.
	authority := assertions.NewAuthority()
	st, err := stack.Open(stack.Options{Root: root, Authority: authority})
	if err != nil {
		log.Fatal(err)
	}
	defer st.Close()
	reviewer := authority.Credential("reviewer")

	kind, err := st.Assertions.Register(ctx, assertions.Kind{
		Name: "example.review", Version: 1, Keyed: true,
		Shape:   objects.Shape{Type: "string"},
		Writers: []string{"reviewer"},
	}, assertions.Writer{})
	if err != nil {
		log.Fatal(err)
	}
	subject := provenance.Ref{URL: "https://example.test/doc", Retrieval: "etag-1"}
	first, err := st.Assertions.Record(ctx, assertions.Request{
		Kind: kind, Key: "doc", Subject: subject, Value: json.RawMessage(`"draft"`), Writer: reviewer,
	})
	if err != nil {
		log.Fatal(err)
	}
	_, err = st.Assertions.Record(ctx, assertions.Request{
		Kind: kind, Key: "doc", Subject: subject, Value: json.RawMessage(`"approved"`), Replaces: first, Writer: reviewer,
	})
	if err != nil {
		log.Fatal(err)
	}
	current, err := st.Assertions.Current(ctx, kind, "doc")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s written by %s\n", current.Value, current.Writer)
}
```

## Semantics, errors, and limits

**Keys.** A kind registered `Keyed` requires a key on every assertion, and an unkeyed
kind rejects one. This is narrower than an "optional caller key": a key that is not
unique has no single current answer, so `Current` could not be one call. A second
assertion on a current key must name the one it replaces, or it fails with
`ErrConflict`. Replacements cannot fork: two writers replacing the same assertion
produce one success and one conflict.

**Retries.** With a caller-chosen `ID`, an identical retry returns the same ID and
different content conflicts, which is what makes [composite retries](durability.md#composite-operations)
safe. `RecordError.Committed` reports whether the record landed.

**Writer authority.** A restricted kind rejects a missing credential, a wrong identity
and a credential from another `Authority`, all with `ErrWriterForbidden`, and records
the authenticated identity on the assertion. `ReservedNamespaces` restricts who may
register under a name prefix, reported as `ErrNamespace`. Reading is unrestricted.

**Threat model.** This is capability distribution, not a sandbox. It stops code that
holds the `Service` handle and any amount of ordinary API access from writing a
restricted kind. It cannot stop in-process code that uses reflection or `unsafe` to
read a credential, that holds the `Authority`, that opens its own service over the root
while no live owner holds it, or that edits journal files. Construct the `Authority` on
a trusted path and hand out only the credentials callers need.

**Queries.** Exactly the filters set are applied, using indexes: `Subject`, `Kind`,
`Key`, `Evidence`, `IncludeEnded`, `AsOf`, `IncludeInternal`. `Limit` defaults to 100
and caps at 1,000. Cursors follow the [shared contract](queries.md#pagination-and-concurrent-updates).
Internal assertions are hidden by default, and this is reporting, not access control.

**Bounds.** A journal defaults to 20,000,000 records and 64 GiB, and a record to 8 MiB;
`MaxRecords` and `MaxBytes` change the first two. Commit durability is described in
[durability](durability.md).

## Validation and evidence

`go test ./assertions` covers the lifecycle and reopen, forks, idempotent retries,
100 racing writers on one key, evidence and as-of queries, occurrence non-bleed, feed
resume, lease exclusion and closed services. `TestForgery` tries the default handle, a
wrong credential, another authority, and direct artifact and lineage writes mimicking
assertion encodings; none produces a visible restricted assertion. These cover
qualification scenarios 2, 3 and 4.
