package extsort

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"os"
	"testing"
)

func TestSortsAcrossSpillsAndCompaction(t *testing.T) {
	dir := t.TempDir()
	// A tiny memory bound forces far more runs than the merge fan-in.
	s := New(Options{Compare: bytes.Compare, MaxMemory: 1024, TempDir: dir})
	defer s.Close()
	const total = 20000
	rng := rand.New(rand.NewSource(1))
	for _, v := range rng.Perm(total) {
		var record [8]byte
		binary.BigEndian.PutUint64(record[:], uint64(v))
		if err := s.Add(record[:]); err != nil {
			t.Fatal(err)
		}
	}
	if s.Count() != total {
		t.Fatal(s.Count())
	}
	for pass := 0; pass < 2; pass++ {
		next := uint64(0)
		if err := s.Each(func(record []byte) error {
			if got := binary.BigEndian.Uint64(record); got != next {
				t.Fatalf("pass %d: got %d want %d", pass, got, next)
			}
			next++
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if next != total {
			t.Fatal(next)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if files, _ := os.ReadDir(dir); len(files) != 0 {
		t.Fatalf("spill files remain: %d", len(files))
	}
}
