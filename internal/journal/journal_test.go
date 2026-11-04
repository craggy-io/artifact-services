package journal

import (
	"context"
	"errors"
	"github.com/craggy-io/artifact-services/internal/canon"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDurableConflictAndLease(t *testing.T) {
	root := t.TempDir()
	s, err := Open(root, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(root, Options{}); err == nil {
		t.Fatal("duplicate lease")
	}
	e, reused, err := s.Append(context.Background(), "id", []byte(`{"x":1.0}`))
	if err != nil || reused || e.Sequence != 1 {
		t.Fatal(e, reused, err)
	}
	_, reused, err = s.Append(context.Background(), "id", []byte(`{"x":1}`))
	if err != nil || !reused {
		t.Fatal(err)
	}
	if _, _, err = s.Append(context.Background(), "id", []byte(`{"x":2}`)); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(root, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	e, ok := s.Get("id")
	if !ok || string(e.Data) != `{"x":1}` {
		t.Fatal(e)
	}
	e.Data[0] = '!'
	e, _ = s.Get("id")
	if e.Data[0] != '{' {
		t.Fatal("aliased data")
	}
}
func TestCancellationCapacityAndCorruption(t *testing.T) {
	root := t.TempDir()
	s, err := Open(root, Options{MaxRecords: 1})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err = s.Append(ctx, "x", []byte(`{}`)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, _, err = s.Append(context.Background(), "x", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if _, _, err = s.Append(context.Background(), "y", []byte(`{}`)); err == nil {
		t.Fatal("capacity")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, "00000000000000000001.json"), []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = Open(root, Options{}); err == nil {
		t.Fatal("corruption accepted")
	}
}
func TestReplayRejectsInvalidCanonicalRecords(t *testing.T) {
	cases := map[string][]byte{"numeric": []byte(`{"data":{"n":1.0},"key":"id","sequence":1}`), "duplicate": []byte(`{"data":{},"key":"id","key":"other","sequence":1}`), "unknown": []byte(`{"data":{},"extra":true,"key":"id","sequence":1}`)}
	longKey := strings.Repeat("x", 1025)
	cases["key"], _ = canon.Marshal(Entry{Sequence: 1, Key: longKey, Data: []byte(`{}`)})
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			s, err := Open(root, Options{})
			if err != nil {
				t.Fatal(err)
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(filepath.Join(root, "00000000000000000001.json"), data, 0600); err != nil {
				t.Fatal(err)
			}
			if _, err = Open(root, Options{}); err == nil {
				t.Fatal("invalid persisted record accepted")
			}
		})
	}
}
func TestOpenRejectsOversizedConfigAndUnexpectedDirectory(t *testing.T) {
	for _, name := range []string{"config", "directory"} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			if name == "config" {
				f, err := os.Create(filepath.Join(root, "config.json"))
				if err != nil {
					t.Fatal(err)
				}
				if err = f.Truncate(9 << 20); err != nil {
					t.Fatal(err)
				}
				f.Close()
			} else {
				if err := os.Mkdir(filepath.Join(root, "bad"), 0700); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := Open(root, Options{}); err == nil {
				t.Fatal("unexpected input accepted")
			}
		})
	}
}
