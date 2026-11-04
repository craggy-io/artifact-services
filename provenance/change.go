package provenance

import (
	"errors"
	"strings"
)

// ErrChangeLimit reports a change page size outside 0..1000.
var ErrChangeLimit = errors.New("provenance: change limit must be 0..1000")

// Change is one committed record in a journal-backed layer's feed. Class, Kind,
// Activity and Actor describe it without decoding the record; empty means the
// class has no such attribute.
type Change struct {
	Sequence uint64 `json:"sequence"`
	Class    string `json:"class"`
	ID       string `json:"id"`
	Kind     string `json:"kind,omitempty"`
	Activity string `json:"activity,omitempty"`
	Actor    string `json:"actor,omitempty"`
	Internal bool   `json:"internal,omitempty"`
}

// ChangeQuery resumes a feed after a sequence. Zero Limit selects 100.
type ChangeQuery struct {
	After           uint64
	Limit           int
	IncludeInternal bool
}

// ChangePage is one ordered slice of a feed. Resume from Last, which advances past
// filtered records too. Fewer than Limit changes means the feed was exhausted when
// read; committed records are never skipped and never reordered.
type ChangePage struct {
	Changes []Change
	Last    uint64
}

// ValidRecordID reports whether a caller-chosen record identifier is acceptable:
// non-empty, at most 256 bytes, no surrounding space or control characters.
func ValidRecordID(id string) bool {
	if len(id) == 0 || len(id) > 256 || strings.TrimSpace(id) != id {
		return false
	}
	for _, r := range id {
		if r < 32 || r == 127 {
			return false
		}
	}
	return true
}
