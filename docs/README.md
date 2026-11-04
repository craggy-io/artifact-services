# Documentation

[Project](../README.md) · [Walkthrough](walkthrough.md)

Storage and observability are implemented. CAS, artifacts, Objects and lineage are proposed designs.

Interface signatures in the layer guides are package-local interface methods unless explicitly shown as
functions/receiver methods for existing APIs; proposed backend constructors and
persistence formats are intentionally not specified. Every domain operation takes
context first. Existing constructors, context attachment, and span/collector methods
retain their actual signatures: spans already carry the operation configuration.

## Layer guides

- [Storage](storage.md)
- [Observability](observability.md)
- [Cas](cas.md)
- [Artifacts](artifacts.md)
- [Objects](objects.md)
- [Lineage](lineage.md)

Each guide uses the same sections: purpose, mental model, responsibilities and boundaries, public interfaces, working with other layers, examples, semantics/errors/limits, and validation/evidence.

## Shared guides

- [Query contracts](queries.md): discovery, traversal, snapshot cursors, index authority and visibility.
- [Walkthrough](walkthrough.md): ingestion, typed snapshots, schema evolution and cross-history derivation.
- [Verification](verification.md): runnable checks and qualified large-file measurements.

## Branding

The two SVG wordmarks in `assets/` are unchanged copies of the Craggy brand kit's light/dark variants. The kit identifies these as transparent, outlined JetBrains Mono Bold artwork, with amber `#C26F00` on light / `#FFB547` on dark. Root README selects the appropriate variant by display theme. Source brand assets were not changed.
