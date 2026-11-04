// Package extsort sorts arbitrarily many records with bounded memory by spilling
// sorted runs to temporary files and merging them.
package extsort

import (
	"bufio"
	"container/heap"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
)

const (
	defaultMemory = 32 << 20
	maxOpenRuns   = 64
	recordHeader  = 64 // Approximate per-record slice overhead.
)

// Options bounds resident memory. Compare orders records; ties have no set order.
type Options struct {
	Compare   func(a, b []byte) int
	MaxMemory int
	TempDir   string
}

// Sorter accepts records with Add, then yields them in order from Each. Records
// are opaque byte strings. Close must always be called to remove spill files.
type Sorter struct {
	options Options
	buffer  [][]byte
	used    int
	runs    []string
	count   int64
	closed  bool
}

// New panics on a nil Compare, which is a programming error.
func New(o Options) *Sorter {
	if o.MaxMemory <= 0 {
		o.MaxMemory = defaultMemory
	}
	if o.Compare == nil {
		panic("extsort: nil Compare")
	}
	return &Sorter{options: o}
}

// Count returns the number of records added.
func (s *Sorter) Count() int64 { return s.count }

// Add copies a record into the buffer, spilling when the memory bound is reached.
func (s *Sorter) Add(record []byte) error {
	if s.closed {
		return errors.New("extsort: closed")
	}
	s.buffer = append(s.buffer, append([]byte(nil), record...))
	s.used += len(record) + recordHeader
	s.count++
	if s.used >= s.options.MaxMemory {
		return s.spill()
	}
	return nil
}

func (s *Sorter) sortBuffer() {
	sort.Slice(s.buffer, func(i, j int) bool { return s.options.Compare(s.buffer[i], s.buffer[j]) < 0 })
}

func (s *Sorter) spill() error {
	s.sortBuffer()
	buffer := s.buffer
	path, err := s.writeRun(func(write func([]byte) error) error {
		for _, record := range buffer {
			if err := write(record); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	s.runs = append(s.runs, path)
	s.buffer, s.used = nil, 0
	if len(s.runs) >= maxOpenRuns {
		return s.compact()
	}
	return nil
}

// compact merges all current runs into one so merge fan-in stays bounded.
func (s *Sorter) compact() error {
	runs := s.runs
	path, err := s.writeRun(func(write func([]byte) error) error { return s.merge(runs, nil, write) })
	if err != nil {
		return err
	}
	s.runs = []string{path}
	return removeAll(runs)
}

func (s *Sorter) writeRun(fill func(func([]byte) error) error) (path string, result error) {
	f, err := os.CreateTemp(s.options.TempDir, "extsort-*")
	if err != nil {
		return "", err
	}
	defer func() {
		result = errors.Join(result, f.Close())
		if result != nil {
			result = errors.Join(result, os.Remove(f.Name()))
		}
	}()
	writer := bufio.NewWriterSize(f, 1<<20)
	var header [binary.MaxVarintLen64]byte
	write := func(record []byte) error {
		n := binary.PutUvarint(header[:], uint64(len(record)))
		if _, err := writer.Write(header[:n]); err != nil {
			return err
		}
		_, err := writer.Write(record)
		return err
	}
	if err := fill(write); err != nil {
		return f.Name(), err
	}
	return f.Name(), writer.Flush()
}

func removeAll(paths []string) error {
	var result error
	for _, path := range paths {
		result = errors.Join(result, os.Remove(path))
	}
	return result
}

type source struct {
	reader  *bufio.Reader
	closer  io.Closer
	current []byte
	memory  [][]byte
}

func (r *source) next() (bool, error) {
	if r.reader == nil {
		if len(r.memory) == 0 {
			return false, nil
		}
		r.current, r.memory = r.memory[0], r.memory[1:]
		return true, nil
	}
	size, err := binary.ReadUvarint(r.reader)
	if err == io.EOF {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if size > 1<<31 {
		return false, fmt.Errorf("extsort: corrupt run record of %d bytes", size)
	}
	record := make([]byte, size)
	if _, err = io.ReadFull(r.reader, record); err != nil {
		return false, err
	}
	r.current = record
	return true, nil
}

type mergeHeap struct {
	sources []*source
	compare func(a, b []byte) int
}

func (h *mergeHeap) Len() int { return len(h.sources) }
func (h *mergeHeap) Less(i, j int) bool {
	return h.compare(h.sources[i].current, h.sources[j].current) < 0
}
func (h *mergeHeap) Swap(i, j int) { h.sources[i], h.sources[j] = h.sources[j], h.sources[i] }
func (h *mergeHeap) Push(x any)    { h.sources = append(h.sources, x.(*source)) }
func (h *mergeHeap) Pop() any {
	last := h.sources[len(h.sources)-1]
	h.sources = h.sources[:len(h.sources)-1]
	return last
}

func (s *Sorter) merge(runs []string, memory [][]byte, visit func([]byte) error) (result error) {
	merged := &mergeHeap{compare: s.options.Compare}
	var open []*source
	defer func() {
		for _, r := range open {
			result = errors.Join(result, r.closer.Close())
		}
	}()
	for _, path := range runs {
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		r := &source{reader: bufio.NewReaderSize(f, 256<<10), closer: f}
		open = append(open, r)
		ok, err := r.next()
		if err != nil {
			return err
		}
		if ok {
			merged.sources = append(merged.sources, r)
		}
	}
	if len(memory) > 0 {
		r := &source{memory: memory}
		if _, err := r.next(); err != nil {
			return err
		}
		merged.sources = append(merged.sources, r)
	}
	heap.Init(merged)
	for merged.Len() > 0 {
		top := merged.sources[0]
		if err := visit(top.current); err != nil {
			return err
		}
		ok, err := top.next()
		if err != nil {
			return err
		}
		if ok {
			heap.Fix(merged, 0)
		} else {
			heap.Pop(merged)
		}
	}
	return nil
}

// Each visits all records in order. The slice is only valid during the call.
// It may be called repeatedly; records remain until Close.
func (s *Sorter) Each(visit func([]byte) error) error {
	if s.closed {
		return errors.New("extsort: closed")
	}
	s.sortBuffer()
	return s.merge(s.runs, s.buffer, visit)
}

// Close removes every spill file. It is safe to call more than once.
func (s *Sorter) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	s.buffer = nil
	err := removeAll(s.runs)
	s.runs = nil
	return err
}
