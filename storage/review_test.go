package storage

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestReviewZeroValueFilesystemRejected(t *testing.T) {
	ctx := context.Background()
	zero := &Filesystem{}
	if err := zero.Put(ctx, "ab", strings.NewReader("x")); err == nil {
		t.Fatal("zero-value Put succeeded")
	}
	if _, err := zero.Open(ctx, "ab"); err == nil {
		t.Fatal("zero-value Open succeeded")
	}
	if _, err := zero.Exists(ctx, "ab"); err == nil {
		t.Fatal("zero-value Exists succeeded")
	}
	if _, err := os.Stat("ab"); !os.IsNotExist(err) {
		t.Fatalf("zero-value Put created CWD-relative blobs: %v", err)
	}
	var nilFS *Filesystem
	if err := nilFS.Put(ctx, "ab", strings.NewReader("x")); err == nil {
		t.Fatal("nil Filesystem Put succeeded")
	}
}

func TestReviewPutErrorNilErrDoesNotPanic(t *testing.T) {
	_ = fmt.Sprintf("%v", &PutError{Published: true})
	var nilErr *PutError
	_ = fmt.Sprintf("%v", nilErr)
}
