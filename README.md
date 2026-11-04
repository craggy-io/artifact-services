<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/assets/craggy-wordmark-on-dark.svg">
  <img src="docs/assets/craggy-wordmark-on-light.svg" alt="Craggy" width="300">
</picture>

# Artifact Services

A streaming Go library for immutable content, artifact snapshots, typed objects,
and provenance. Small interfaces keep physical storage separate from content
identity, typed meaning, and revision histories. Context carries cancellation and
shared observation across operations.

Storage and observability are implemented; CAS, artifacts, Objects, and lineage
are proposed API designs, not available packages. Objects and lineage are peers
over artifacts; artifacts builds on CAS, which builds on storage.

| Layer | Responsibilities | Guide |
| --- | --- | --- |
| Storage | Immutable keyed blobs, streaming I/O and atomic publication. | [Storage](docs/storage.md) |
| Observability | Context scopes, progress, semantic counters and internal-work filtering. | [Observability](docs/observability.md) |
| CAS | Content identity, deduplication and verified streaming reads. | [CAS](docs/cas.md) |
| Artifacts | Immutable manifests, file access and catalog discovery. | [Artifacts](docs/artifacts.md) |
| Objects | Runtime schemas, typed validation and content-query adapters. | [Objects](docs/objects.md) |
| Lineage | Generic activities, independent revisions and row/column provenance. | [Lineage](docs/lineage.md) |

Start with the [documentation index](docs/README.md), the
[runnable storage/observation example](docs/observability.md#examples), or the
[illustrative end-to-end walkthrough](docs/walkthrough.md).

## Development

Requires Go 1.27 or newer; no external dependencies.

```sh
go test ./...
go test -race ./...
go vet ./...
go build ./...
```

See [verification and measurements](docs/verification.md) for Windows failure
coverage, the opt-in 1 GiB streaming trial, and its measurement limitations.
