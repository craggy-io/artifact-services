# Documentation

[Project](../README.md) · [Walkthrough](walkthrough.md)

All seven layers have Go implementations. Each guide describes the available
API, supported subset, resource bounds, and verification limits. Domain operations
accept context first; constructors and context/span helpers retain their documented
signatures. See the runnable [cross-layer walkthrough](walkthrough.md) and
[verification evidence](verification.md).

## Layer guides

- [Storage](storage.md)
- [Observability](observability.md)
- [CAS](cas.md)
- [Artifacts](artifacts.md)
- [Objects](objects.md)
- [Lineage](lineage.md)
- [Assertions](assertions.md)

Each guide uses the same sections: purpose, mental model, responsibilities and boundaries, public interfaces, working with other layers, examples, semantics/errors/limits, and validation/evidence.

## Shared guides

- [Query contracts](queries.md): discovery, traversal, snapshot cursors, index authority and visibility.
- [Walkthrough](walkthrough.md): ingestion, typed snapshots, schema evolution and cross-history derivation.
- [Durability](durability.md): what success promises, crash recovery, and retryable composite operations.
- [Adapters](adapters.md): optional packages for wiring, runs, typed access, tree I/O, spooling, GC, testing and SQL.
- [Stability](stability.md): API drift guard, releases and on-disk format policy.
- [Verification](verification.md): runnable checks, crash qualification and qualified large-scale measurements.
- [Successor requirements](successor-requirements.md): a consumer's (CC Lab) requirements list for replacing cclab-storage with artifact-services — what must be core, what generic adapters, and what stays consumer-side.
- [Successor decisions](successor-analysis.md): the response to that list, including where it disagrees.

## Branding

The two SVG wordmarks in `assets/` are unchanged copies of the Craggy brand kit's light/dark variants. The kit identifies these as transparent, outlined JetBrains Mono Bold artwork, with amber `#C26F00` on light / `#FFB547` on dark. Root README selects the appropriate variant by display theme. Source brand assets were not changed.
