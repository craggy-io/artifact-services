package artifacts

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"unicode/utf8"

	"github.com/craggy-io/artifact-services/cas"
	"github.com/craggy-io/artifact-services/internal/canon"
)

const (
	// ManifestVersion is the stored layout version. Reads refuse any other value.
	ManifestVersion = 1

	maxMetadataBytes = 1 << 20
	// Blocks are split deterministically by these bounds, so equal entries always
	// produce equal blocks, and so an equal artifact ID, whatever the insert order.
	maxBlockEntries = 4096
	maxBlockBytes   = 1 << 20
	maxModeBits     = 0o7777
)

// A stored manifest is a root object naming content-addressed entry blocks. Only
// the root and the block holding a path are read to resolve that path.
type blockRef struct {
	ID    cas.ID `json:"id"`
	First string `json:"first"`
	Count int    `json:"count"`
}
type storedManifest struct {
	Version   int                        `json:"version"`
	Entries   int64                      `json:"entries"`
	Files     int64                      `json:"files"`
	Bytes     int64                      `json:"bytes"`
	Sole      cas.ID                     `json:"sole,omitempty"`
	LinkDepth int                        `json:"link_depth,omitempty"`
	Blocks    []blockRef                 `json:"blocks"`
	Metadata  map[string]json.RawMessage `json:"metadata,omitempty"`
}

// contentID computes a snapshot's content identity. See CatalogItem.Content.
func (m storedManifest) contentID() (cas.ID, error) {
	if m.Sole != "" {
		return m.Sole, nil
	}
	encoded, err := canon.Marshal(m.Blocks)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return cas.ID("sha256:" + hex.EncodeToString(sum[:])), nil
}

func validPath(path string) bool {
	if path == "" || len(path) > 4096 || !utf8.ValidString(path) || strings.ContainsAny(path, "\\\x00") || strings.HasPrefix(path, "/") {
		return false
	}
	for _, part := range strings.Split(path, "/") {
		if part == "" || part == "." || part == ".." || strings.Contains(part, ":") {
			return false
		}
	}
	return true
}

// comparePaths orders by path component: a directory's descendants sort directly
// after it, which keeps a subtree contiguous for paging, link expansion and the
// file-versus-directory conflict check. "a" < "a/b" < "a-b".
func comparePaths(a, b string) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		x, y := a[i], b[i]
		switch {
		case x == y:
		case x == '/':
			return -1
		case y == '/':
			return 1
		case x < y:
			return -1
		default:
			return 1
		}
	}
	switch {
	case len(a) < len(b):
		return -1
	case len(a) > len(b):
		return 1
	}
	return 0
}

func invalidf(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrInvalidManifest}, args...)...)
}

// validateEntry checks one entry alone. Ordering and link targets are checked by
// the publication pipeline, which sees entries in sequence.
func validateEntry(e Entry) error {
	if !validPath(e.Path) {
		return invalidf("path or size: invalid path %q", e.Path)
	}
	if e.Size < 0 {
		return invalidf("path or size: negative size for %q", e.Path)
	}
	if e.Mode > maxModeBits {
		return invalidf("mode %o for %q", e.Mode, e.Path)
	}
	switch e.Kind {
	case "":
		if _, err := cas.ParseID(string(e.Content)); err != nil {
			return invalidf("content ID: %w", err)
		}
		if e.Target != "" || e.TargetPath != "" {
			return invalidf("file %q has a link target", e.Path)
		}
	case KindDir:
		if e.Content != "" || e.Size != 0 || e.Target != "" || e.TargetPath != "" {
			return invalidf("directory %q has file or link fields", e.Path)
		}
	case KindLink:
		if e.Content != "" || e.Size != 0 {
			return invalidf("link %q has file fields", e.Path)
		}
		if _, err := ParseID(string(e.Target)); err != nil {
			return invalidf("link target: %w", err)
		}
		if e.TargetPath != "" && !validPath(e.TargetPath) {
			return invalidf("link target path %q", e.TargetPath)
		}
	default:
		return invalidf("entry kind %q", e.Kind)
	}
	return nil
}

// checkOrder rejects duplicates and any entry nested under a non-directory. Under
// comparePaths such a descendant is always the immediate successor.
func checkOrder(previous, current Entry) error {
	switch comparePaths(previous.Path, current.Path) {
	case 0:
		return invalidf("duplicate path %q", current.Path)
	case 1:
		return invalidf("entries out of order at %q", current.Path)
	}
	if previous.Kind != KindDir && strings.HasPrefix(current.Path, previous.Path+"/") {
		return invalidf("%q is nested under non-directory %q", current.Path, previous.Path)
	}
	return nil
}

func normalizeMetadata(input map[string]json.RawMessage, allowClaims bool) (map[string]json.RawMessage, error) {
	if len(input) > 256 {
		return nil, invalidf("metadata key limit")
	}
	if len(input) == 0 {
		return nil, nil
	}
	result := make(map[string]json.RawMessage, len(input))
	total := 0
	for key, value := range input {
		total += len(key) + len(value)
		if total > maxMetadataBytes {
			return nil, invalidf("input metadata byte limit")
		}
		if key == "" || len(key) > 256 || !utf8.ValidString(key) || strings.ContainsRune(key, 0) {
			return nil, invalidf("metadata key")
		}
		if !allowClaims && strings.HasPrefix(key, "objects.") {
			return nil, invalidf("reserved objects claim")
		}
		normalized, err := canon.Normalize(value)
		if err != nil {
			return nil, invalidf("metadata %q: %w", key, err)
		}
		result[key] = normalized
	}
	encoded, err := canon.Marshal(result)
	if err != nil {
		return nil, err
	}
	if len(encoded) > maxMetadataBytes {
		return nil, invalidf("metadata byte limit")
	}
	return result, nil
}

// blockWriter groups a sorted entry stream into blocks. flush receives each block.
type blockWriter struct {
	flush    func(encoded []byte, first string, count int) error
	buffer   bytes.Buffer
	first    string
	count    int
	previous *Entry
	root     storedManifest
	soleSeen bool
}

func (w *blockWriter) add(e Entry) error {
	if w.previous != nil {
		if err := checkOrder(*w.previous, e); err != nil {
			return err
		}
	}
	encoded, err := canon.Marshal(e)
	if err != nil {
		return err
	}
	if w.count > 0 && (w.count >= maxBlockEntries || w.buffer.Len()+len(encoded)+1 > maxBlockBytes) {
		if err := w.emit(); err != nil {
			return err
		}
	}
	if w.count == 0 {
		w.first = e.Path
	}
	w.buffer.Write(encoded)
	w.buffer.WriteByte('\n')
	w.count++
	w.root.Entries++
	if e.IsFile() {
		if e.Size > math.MaxInt64-w.root.Bytes {
			return invalidf("path or size: total size overflow")
		}
		w.root.Files++
		w.root.Bytes += e.Size
		w.root.Sole = e.Content
	}
	previous := e
	w.previous = &previous
	return nil
}

func (w *blockWriter) emit() error {
	if w.count == 0 {
		return nil
	}
	err := w.flush(w.buffer.Bytes(), w.first, w.count)
	w.buffer.Reset()
	w.count = 0
	return err
}

// finish flushes the last block and settles fields only known at the end.
func (w *blockWriter) finish() error {
	if err := w.emit(); err != nil {
		return err
	}
	if w.root.Entries != 1 || w.root.Files != 1 {
		w.root.Sole = ""
	}
	if w.root.Blocks == nil {
		w.root.Blocks = []blockRef{}
	}
	w.root.Version = ManifestVersion
	return nil
}

func decodeRoot(data []byte, limit int64) (storedManifest, error) {
	if int64(len(data)) > limit {
		return storedManifest{}, invalidf("byte limit")
	}
	var root storedManifest
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&root); err != nil {
		return storedManifest{}, invalidf("%w", err)
	}
	if root.Version != ManifestVersion {
		return storedManifest{}, invalidf("version %d", root.Version)
	}
	metadata, err := normalizeMetadata(root.Metadata, true)
	if err != nil {
		return storedManifest{}, err
	}
	root.Metadata = metadata
	encoded, err := canon.Marshal(root)
	if err != nil {
		return storedManifest{}, invalidf("%w", err)
	}
	if !bytes.Equal(data, encoded) {
		return storedManifest{}, invalidf("noncanonical bytes")
	}
	total := int64(0)
	for i, block := range root.Blocks {
		if _, err := cas.ParseID(string(block.ID)); err != nil {
			return storedManifest{}, invalidf("block ID: %w", err)
		}
		if block.Count < 1 || !validPath(block.First) || i > 0 && comparePaths(root.Blocks[i-1].First, block.First) >= 0 {
			return storedManifest{}, invalidf("block index")
		}
		total += int64(block.Count)
	}
	if total != root.Entries {
		return storedManifest{}, invalidf("block entry count")
	}
	return root, nil
}

func decodeBlock(data []byte, ref blockRef) ([]Entry, error) {
	entries := make([]Entry, 0, ref.Count)
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(nil, maxBlockBytes+maxBlockEntries)
	for scanner.Scan() {
		line := scanner.Bytes()
		normalized, err := canon.Normalize(line)
		if err != nil {
			return nil, invalidf("block entry: %w", err)
		}
		if !bytes.Equal(line, normalized) {
			return nil, invalidf("noncanonical block entry")
		}
		var e Entry
		decoder := json.NewDecoder(bytes.NewReader(line))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&e); err != nil {
			return nil, invalidf("block entry: %w", err)
		}
		if err := validateEntry(e); err != nil {
			return nil, err
		}
		if n := len(entries); n > 0 {
			if err := checkOrder(entries[n-1], e); err != nil {
				return nil, err
			}
		}
		entries = append(entries, e)
	}
	if err := scanner.Err(); err != nil {
		return nil, invalidf("block: %w", err)
	}
	if len(entries) != ref.Count || entries[0].Path != ref.First {
		return nil, invalidf("block does not match its index")
	}
	return entries, nil
}

func readBounded(reader io.ReadCloser, limit int64) (data []byte, err error) {
	data, err = io.ReadAll(io.LimitReader(reader, limit+1))
	err = errors.Join(err, reader.Close())
	if int64(len(data)) > limit {
		err = errors.Join(err, invalidf("byte limit"))
	}
	return data, err
}
