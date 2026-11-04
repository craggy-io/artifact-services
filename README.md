# artifact-services

A Go library for artifact services. Dependency direction: **artifacts → CAS → storage**.
Only storage is implemented; there is no server or CLI.

| Layer | Responsibilities | Public interfaces |
| --- | --- | --- |
| `storage` | Physical layout, streaming reads/writes, existence checks, atomic immutable publication; one file per blob. | <ul><li><code>NewFilesystem(root string) (*Filesystem, error)</code></li><li><code>Put(key string, source io.Reader) error</code></li><li><code>Open(key string) (io.ReadCloser, error)</code></li><li><code>Exists(key string) (bool, error)</code></li></ul> |
| CAS (`cas`) | Hashing and content identity over storage: store/read content by digest, verify identity, and deduplicate logical writes. | |
| `artifacts` | Organize artifacts over CAS: publish/read artifact manifests and metadata. | |

## Usage

```go
import (
    "io"
    "strings"

    "github.com/craggy-io/artifact-services/storage"
)

func example(root string, output io.Writer) error {
    backend, err := storage.NewFilesystem(root)
    if err != nil { return err }
    var blobs storage.Store = backend
    if err := blobs.Put("abcd0123", strings.NewReader("hello")); err != nil {
        return err
    }
    reader, err := blobs.Open("abcd0123")
    if err != nil { return err }
    defer reader.Close()
    _, err = io.Copy(output, reader)
    return err
}
```

Callers own returned readers and must close them. Source readers are never closed
by storage. Subsequent read/close errors come from the returned reader. Streaming
I/O allows callers to wrap a source with a counting reader for byte progress;
bytes consumed do not mean publication succeeded. No progress callback API exists.

## Storage contract

- Keys are 2–128 ASCII lowercase hexadecimal characters, not verified digests.
  Uppercase, separators, traversal, and other characters return `ErrInvalidKey`.
- Physical layout is `<root>/<first-two>/<key>`, with one file per blob. Paths
  are internal, although filesystem errors may include paths in diagnostics.
- `Put` never overwrites. Concurrent completed writes to one absent key have
  one winner; losers return `ErrAlreadyExists`. A duplicate may consume its
  source. Source/filesystem errors can occur before duplicate detection.
- `Open` returns `ErrNotFound` for missing blobs; `Exists` returns `false, nil`.
  Other errors propagate; non-regular blob entries are errors. Match storage
  sentinel errors with `errors.Is`. Missing and duplicate errors also preserve
  their underlying OS errors for `errors.Is` and `errors.As`; permission failures
  are returned without being converted into missing or duplicate errors.
- A write streams to a temporary file in the destination shard, syncs and closes
  it, then publishes a hard link without replacement. Readers see absence or
  complete content. Source, write, sync, close, or publication failures never
  publish partial bytes or replace existing blobs.
- Temporary files are removed on normal success/failure. Cleanup errors are
  returned, joined with prior errors. A cleanup failure after publication returns
  an error even though the complete blob is visible. Empty shards may remain.

The backend supports concurrent goroutines and independent instances sharing a
root. Keep the directory tree private/trusted: external edits, symlinks, and
junctions within it are unsupported. No deletion/lifecycle API is provided.
If an empty root is removed between calls, reads report missing and a subsequent
`Put` recreates the directory tree if permissions allow it.

Any configured root, including a Docker-mounted directory, is usable if its
filesystem supports atomic, non-replacing hard-link creation (such as local NTFS).
Unsupported filesystems return errors; there is no copy/overwrite fallback.
Mount/network filesystem semantics require deployment verification. File contents
are synced before publication, but directory entries are not synced; crash or
power-loss durability is not guaranteed. Process termination can leave temporary
files; there is no automatic recovery that could interfere with active writers.

## Development

Requires Go 1.27 or newer; no external dependencies.

```sh
go fmt ./...
go test ./...
go test -race ./...
go vet ./...
go build ./...
```

Tests cover empty/binary roundtrips, reopening, immutable writes, key length
boundaries, invalid characters/traversal, spaces/Unicode in root names, root/shard
conflicts, non-regular blob entries, deleted-root recreation, closed sources,
interrupted streams, failed replacements, publication failures, cleanup,
pending-write invisibility, concurrent same-key publication, and OS error
preservation.

Windows-specific tests exercise actual roundtrips beyond the legacy 260-character
path limit and OS rejection of invalid root characters. Local verification passed
with a 321-character root and a 453-character blob path. ACL tests deny temporary
file creation and blob reads within disposable fixtures, then restore those ACLs.
These tests skip only if `icacls.exe` is unavailable; no tests were skipped in
local Windows verification. These results do not qualify every filesystem/mount
or arbitrary path lengths. Disk-full, sync/close failures, cleanup failures,
and crash recovery have not been fault-injected or verified.
