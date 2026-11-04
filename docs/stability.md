# API and format stability

[Project](../README.md) · [Documentation index](README.md)

## Exported API

The module is pre-1.0. Until a v1 tag exists, an exported name can change in any release,
and a consumer should pin an exact version. Every such change is still deliberate:
[`api.golden.txt`](../api.golden.txt) lists the exported surface of the core and adapter
packages, and `TestExportedAPIMatchesGolden` fails on any difference. To make an intended
change, run:

```sh
ARTIFACT_SERVICES_UPDATE_GOLDEN=1 go test ./integration -run TestExportedAPIMatchesGolden
```

Then review the golden diff alongside the code. Anything under `internal/` carries no
promise. `artifacts.PublishValidated` takes an internal type on purpose, so consumers
cannot call it. `adapters/sqlindex` is a separate module and is outside the golden; its
versioned promise is its [schema](adapters.md#sqlindex).

## Releases

Releases are tags following semantic versioning: `vX.Y.Z` for this module and
`adapters/sqlindex/vX.Y.Z` for the nested one, tagged together. A release changing an
on-disk format says so in its notes. No release has been tagged yet, so until one is,
consumers depend on pseudo-versions.

## On-disk formats

The policy for every format is **refuse with an error**. A build never guesses at a format
it does not know, and there is no silent migrate-on-open. A format change brings a new
version and a separate, explicit migration if the data needs one.

| Data | Version marker | Unknown version |
| --- | --- | --- |
| Journal log | `ASJOURNAL1` header | `journal.ErrFormat` on open |
| Manifest | `artifacts.ManifestVersion` in the stored root | Refused on read |
| Journal config | Written once per root, compared on open | Mismatch is an error |
| Objects schemas | `objects.SchemaVersion` | Rejected |
| Reference grammar | `provenance.GrammarVersion`, `Ref.Version` | `provenance.ErrUnsupportedForm` |
| SQL index | `sqlindex.SchemaVersion` | `ErrSchemaVersion`; rebuild from journals |

Storage's layout and CAS content are identified by digest and have no version of their own.
Everything derived, including catalog indexes and the SQL index, is rebuilt rather than
migrated.
