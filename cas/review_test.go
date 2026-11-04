package cas

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/craggy-io/artifact-services/storage"
)

func TestReviewZeroAndTypedNilRejected(t *testing.T) {
	if _, err := New((*storage.Filesystem)(nil), Options{}); err == nil {
		t.Fatal("typed-nil backend accepted")
	}
	zero := &Service{}
	if _, err := zero.Put(context.Background(), strings.NewReader("x")); err == nil {
		t.Fatal("zero-value Put succeeded")
	}
	if _, err := zero.Open(context.Background(), ID("sha256:"+strings.Repeat("0", 64))); err == nil {
		t.Fatal("zero-value Open succeeded")
	}
	if _, err := zero.Exists(context.Background(), ID("sha256:"+strings.Repeat("0", 64))); err == nil {
		t.Fatal("zero-value Exists succeeded")
	}
	_ = fmt.Sprintf("%v", &PutError{Committed: true})
}

type cyclicError struct{}

func (cyclicError) Error() string { return "cycle" }
func (c cyclicError) Unwrap() error {
	return errors.Join(storage.ErrAlreadyExists, c)
}

func TestReviewOnlyDuplicateCycleBounded(t *testing.T) {
	if onlyDuplicate(cyclicError{}) {
		t.Fatal("cyclic error reported duplicate-only")
	}
	if !onlyDuplicate(storage.ErrAlreadyExists) {
		t.Fatal("plain duplicate not recognized")
	}
}

func TestReviewMaxBytesCap(t *testing.T) {
	blobs, err := storage.NewFilesystem(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	svc, err := New(blobs, Options{TempDir: t.TempDir(), MaxBytes: 3})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Put(context.Background(), strings.NewReader("abcd")); !errors.Is(err, ErrLimit) {
		t.Fatalf("oversize Put = %v, want ErrLimit", err)
	}
	res, err := svc.Put(context.Background(), strings.NewReader("abc"))
	if err != nil {
		t.Fatal(err)
	}
	rc, err := svc.Open(context.Background(), res.ID)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(rc)
	if err != nil || string(data) != "abc" {
		t.Fatalf("roundtrip = %q, %v", data, err)
	}
	if err := rc.Close(); err != nil {
		t.Fatal(err)
	}
}
