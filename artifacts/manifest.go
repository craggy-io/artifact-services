package artifacts

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/craggy-io/artifact-services/cas"
	"github.com/craggy-io/artifact-services/internal/canon"
)

const (
	MaxManifestBytes = 8 << 20
	MaxEntries       = 10000
	maxMetadataBytes = 1 << 20
)

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

func normalizeManifest(input Manifest, allowClaims bool) (Manifest, []byte, error) {
	if input.Version != 0 && input.Version != 1 {
		return Manifest{}, nil, fmt.Errorf("%w: unsupported version", ErrInvalidManifest)
	}
	if len(input.Entries) > MaxEntries {
		return Manifest{}, nil, fmt.Errorf("%w: entry limit", ErrInvalidManifest)
	}
	result := Manifest{Version: 1, Entries: append([]Entry{}, input.Entries...)}
	sort.Slice(result.Entries, func(i, j int) bool { return result.Entries[i].Path < result.Entries[j].Path })
	total := int64(0)
	for i, e := range result.Entries {
		if !validPath(e.Path) || e.Size < 0 || e.Size > math.MaxInt64-total {
			return Manifest{}, nil, fmt.Errorf("%w: path or size", ErrInvalidManifest)
		}
		if _, err := cas.ParseID(string(e.Content)); err != nil {
			return Manifest{}, nil, fmt.Errorf("%w: content ID: %w", ErrInvalidManifest, err)
		}
		if i > 0 && result.Entries[i-1].Path == e.Path {
			return Manifest{}, nil, fmt.Errorf("%w: duplicate path", ErrInvalidManifest)
		}
		total += e.Size
	}
	if len(input.Metadata) > 256 {
		return Manifest{}, nil, fmt.Errorf("%w: metadata key limit", ErrInvalidManifest)
	}
	if len(input.Metadata) > 0 {
		result.Metadata = make(map[string]json.RawMessage, len(input.Metadata))
	}
	metadataBytes := 0
	for key, value := range input.Metadata {
		metadataBytes += len(key) + len(value)
		if metadataBytes > maxMetadataBytes {
			return Manifest{}, nil, fmt.Errorf("%w: input metadata byte limit", ErrInvalidManifest)
		}
		if key == "" || len(key) > 256 || !utf8.ValidString(key) || strings.ContainsRune(key, 0) {
			return Manifest{}, nil, fmt.Errorf("%w: metadata key", ErrInvalidManifest)
		}
		if !allowClaims && strings.HasPrefix(key, "objects.") {
			return Manifest{}, nil, fmt.Errorf("%w: reserved objects claim", ErrInvalidManifest)
		}
		normalized, err := canon.Normalize(value)
		if err != nil {
			return Manifest{}, nil, fmt.Errorf("%w: metadata %q: %w", ErrInvalidManifest, key, err)
		}
		result.Metadata[key] = normalized
	}
	metadata, err := canon.Marshal(result.Metadata)
	if err != nil {
		return Manifest{}, nil, err
	}
	if len(metadata) > maxMetadataBytes {
		return Manifest{}, nil, fmt.Errorf("%w: metadata byte limit", ErrInvalidManifest)
	}
	encoded, err := canon.Marshal(result)
	if err != nil {
		return Manifest{}, nil, err
	}
	if len(encoded) > MaxManifestBytes {
		return Manifest{}, nil, fmt.Errorf("%w: manifest byte limit", ErrInvalidManifest)
	}
	return result, encoded, nil
}

func decodeManifest(data []byte) (Manifest, error) {
	if len(data) > MaxManifestBytes {
		return Manifest{}, fmt.Errorf("%w: byte limit", ErrInvalidManifest)
	}
	var result Manifest
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&result); err != nil {
		return Manifest{}, fmt.Errorf("%w: %w", ErrInvalidManifest, err)
	}
	if result.Version != 1 {
		return Manifest{}, fmt.Errorf("%w: version", ErrInvalidManifest)
	}
	normalized, encoded, err := normalizeManifest(result, true)
	if err != nil {
		return Manifest{}, err
	}
	if !bytes.Equal(data, encoded) {
		return Manifest{}, fmt.Errorf("%w: noncanonical bytes", ErrInvalidManifest)
	}
	return normalized, nil
}

func indexedScalar(raw json.RawMessage) (json.RawMessage, error) {
	normalized, err := canon.Normalize(raw)
	if err != nil {
		return nil, err
	}
	var value any
	dec := json.NewDecoder(bytes.NewReader(normalized))
	dec.UseNumber()
	if err := dec.Decode(&value); err != nil {
		return nil, err
	}
	switch x := value.(type) {
	case string:
		if len(x) > 256 {
			return nil, fmt.Errorf("indexed string exceeds 256 bytes")
		}
	case bool:
	case json.Number:
		if _, err := x.Int64(); err != nil {
			return nil, fmt.Errorf("indexed number must be signed int64")
		}
	default:
		return nil, fmt.Errorf("indexed value must be string, bool or int64")
	}
	return normalized, nil
}

func readBounded(reader io.ReadCloser, limit int64) (data []byte, err error) {
	data, err = io.ReadAll(io.LimitReader(reader, limit+1))
	err = errors.Join(err, reader.Close())
	if int64(len(data)) > limit {
		err = errors.Join(err, fmt.Errorf("%w: byte limit", ErrInvalidManifest))
	}
	return data, err
}
