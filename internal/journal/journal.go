// Package journal owns bounded durable immutable records and an exclusive writer lease.
package journal

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/craggy-io/artifact-services/internal/canon"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"
)

var ErrConflict = errors.New("journal: conflicting identity")
var ErrClosed = errors.New("journal: closed")
var ErrCapacity = errors.New("journal: capacity exceeded")

type CommitError struct {
	Committed bool
	Err       error
}

func (e *CommitError) Error() string {
	return fmt.Sprintf("journal committed=%t: %v", e.Committed, e.Err)
}
func (e *CommitError) Unwrap() error { return e.Err }

type Options struct {
	Config     []byte
	MaxBytes   int64
	MaxRecords int
}
type Entry struct {
	Sequence uint64          `json:"sequence"`
	Key      string          `json:"key"`
	Data     json.RawMessage `json:"data"`
}
type Store struct {
	mu           sync.Mutex
	root         string
	lease        *os.File
	options      Options
	entries      map[string]Entry
	sequence     uint64
	bytes        int64
	closed       bool
	leaseRemoved bool
}

func Open(root string, o Options) (result *Store, err error) {
	if root == "" {
		return nil, errors.New("journal: empty root")
	}
	root, err = filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if o.MaxBytes == 0 {
		o.MaxBytes = 128 << 20
	}
	if o.MaxRecords == 0 {
		o.MaxRecords = 100000
	}
	if o.MaxBytes < 1 || o.MaxRecords < 1 {
		return nil, errors.New("journal: invalid limits")
	}
	if err = os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	lease, err := os.OpenFile(filepath.Join(root, ".lease"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, fmt.Errorf("journal exclusive lease (close other owner; crashed-owner leases require manual recovery): %w", err)
	}
	s := &Store{root: root, lease: lease, options: o, entries: map[string]Entry{}}
	defer func() {
		if err != nil {
			err = errors.Join(err, s.Close())
		}
	}()
	config := o.Config
	if len(config) == 0 {
		config = []byte("{}")
	}
	config, err = canon.Normalize(config)
	if err != nil {
		return nil, err
	}
	cp := filepath.Join(root, "config.json")
	existing, e := readBounded(cp, 8<<20)
	if errors.Is(e, os.ErrNotExist) {
		_, err = publish(cp, config)
	} else if e != nil {
		err = e
	} else if !bytes.Equal(config, existing) {
		err = errors.New("journal: configuration differs from persisted policy")
	}
	if err != nil {
		return nil, err
	}
	files, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		if f.Name() == "config.json" || f.Name() == ".lease" || strings.HasPrefix(f.Name(), ".tmp-") {
			continue
		}
		if f.IsDir() {
			return nil, fmt.Errorf("journal: unexpected directory %s", f.Name())
		}
		info, e := f.Info()
		if e != nil {
			return nil, e
		}
		if info.Size() > 8<<20 {
			return nil, errors.New("journal: record size limit")
		}
		data, e := readBounded(filepath.Join(root, f.Name()), 8<<20)
		if e != nil {
			return nil, e
		}
		entry, e := decodeEntry(data)
		if e != nil {
			return nil, e
		}
		if entry.Sequence != s.sequence+1 || f.Name() != fmt.Sprintf("%020d.json", entry.Sequence) {
			return nil, errors.New("journal: corrupt sequence/record")
		}
		if _, ok := s.entries[entry.Key]; ok {
			return nil, ErrConflict
		}
		s.bytes += int64(len(data))
		if s.bytes > o.MaxBytes || len(s.entries) >= o.MaxRecords {
			return nil, ErrCapacity
		}
		s.sequence = entry.Sequence
		s.entries[entry.Key] = entry
	}
	return s, nil
}
func clone(e Entry) Entry { e.Data = append(json.RawMessage(nil), e.Data...); return e }
func (s *Store) Get(key string) (Entry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[key]
	return clone(e), ok
}
func (s *Store) Snapshot() ([]Entry, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := make([]Entry, 0, len(s.entries))
	for _, e := range s.entries {
		a = append(a, clone(e))
	}
	sort.Slice(a, func(i, j int) bool { return a[i].Sequence < a[j].Sequence })
	return a, s.sequence
}
func (s *Store) Append(ctx context.Context, key string, data []byte) (Entry, bool, error) {
	if err := ctx.Err(); err != nil {
		return Entry{}, false, err
	}
	if err := requireKey(key); err != nil {
		return Entry{}, false, err
	}
	if len(data) > 8<<20 {
		return Entry{}, false, errors.New("journal: record size limit")
	}
	canonical, err := canon.Normalize(data)
	if err != nil {
		return Entry{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return Entry{}, false, ErrClosed
	}
	if old, ok := s.entries[key]; ok {
		if bytes.Equal(old.Data, canonical) {
			return clone(old), true, nil
		}
		return Entry{}, false, ErrConflict
	}
	e := Entry{Sequence: s.sequence + 1, Key: key, Data: canonical}
	encoded, err := canon.Marshal(e)
	if err != nil {
		return Entry{}, false, err
	}
	if len(encoded) > 8<<20 || s.bytes+int64(len(encoded)) > s.options.MaxBytes || len(s.entries) >= s.options.MaxRecords {
		return Entry{}, false, ErrCapacity
	}
	if err = ctx.Err(); err != nil {
		return Entry{}, false, err
	}
	committed, err := publish(filepath.Join(s.root, fmt.Sprintf("%020d.json", e.Sequence)), encoded)
	if committed {
		s.entries[key] = e
		s.sequence = e.Sequence
		s.bytes += int64(len(encoded))
	}
	if err != nil {
		return clone(e), false, &CommitError{Committed: committed, Err: err}
	}
	return clone(e), false, nil
}
func publish(path string, data []byte) (committed bool, result error) {
	f, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return false, err
	}
	closed := false
	defer func() {
		if !closed {
			result = errors.Join(result, f.Close())
		}
		result = errors.Join(result, os.Remove(f.Name()))
	}()
	if _, err = io.Copy(f, bytes.NewReader(data)); err != nil {
		return false, err
	}
	if err = f.Sync(); err != nil {
		return false, err
	}
	err = f.Close()
	closed = true
	if err != nil {
		return false, err
	}
	if err = os.Link(f.Name(), path); err != nil {
		return false, err
	}
	return true, nil
}

// Close stops appends and releases the lease. It is retryable: if closing or
// removing the lease file fails, a later Close retries the unfinished step
// instead of reporting success while the stale lease still blocks Open.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.root == "" {
		return ErrClosed
	}
	if s.closed && s.lease == nil && s.leaseRemoved {
		return nil
	}
	s.closed = true
	var result error
	if s.lease != nil {
		if err := s.lease.Close(); err == nil || errors.Is(err, os.ErrClosed) {
			s.lease = nil
		} else {
			result = errors.Join(result, err)
		}
	}
	if !s.leaseRemoved {
		if err := os.Remove(filepath.Join(s.root, ".lease")); err == nil || errors.Is(err, os.ErrNotExist) {
			s.leaseRemoved = true
		} else {
			result = errors.Join(result, err)
		}
	}
	return result
}

func requireKey(key string) error {
	if key == "" || len(key) > 1024 || !utf8.ValidString(key) {
		return errors.New("journal: key limit")
	}
	return nil
}
func decodeEntry(data []byte) (Entry, error) {
	canonical, err := canon.Normalize(data)
	if err != nil {
		return Entry{}, err
	}
	if !bytes.Equal(data, canonical) {
		return Entry{}, errors.New("journal: noncanonical record")
	}
	var e Entry
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&e); err != nil {
		return Entry{}, err
	}
	if err = requireKey(e.Key); err != nil {
		return Entry{}, err
	}
	if len(e.Data) == 0 || !json.Valid(e.Data) {
		return Entry{}, errors.New("journal: invalid data")
	}
	return e, nil
}

func readBounded(path string, limit int64) (data []byte, result error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { result = errors.Join(result, f.Close()) }()
	data, err = io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, errors.New("journal: file size limit")
	}
	return data, nil
}
