package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// Use separate processes so this contract cannot accidentally depend on a Go
// mutex shared by Filesystem instances. CAS relies on this for concurrent dedup.
func TestCrossProcessPublication(t *testing.T) {
	const helperRoot = "ARTIFACT_STORAGE_PUBLICATION_ROOT"
	const helperWriter = "ARTIFACT_STORAGE_PUBLICATION_WRITER"
	const size = 256 * 1024
	if root := os.Getenv(helperRoot); root != "" {
		writer, err := strconv.Atoi(os.Getenv(helperWriter))
		if err == nil {
			_, err = io.ReadFull(os.Stdin, make([]byte, 1))
		}
		var s *Filesystem
		if err == nil {
			s, err = NewFilesystem(root)
		}
		if err == nil {
			err = s.Put(context.Background(), "abcd", bytes.NewReader(bytes.Repeat([]byte{byte(writer)}, size)))
		}
		if err == nil {
			os.Exit(0)
		}
		var outcome *PutError
		if errors.As(err, &outcome) && !outcome.Published && errors.Is(err, ErrAlreadyExists) {
			os.Exit(3)
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	s, root := newTestStore(t)
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const writers = 6
	commands := make([]*exec.Cmd, writers)
	gates := make([]io.WriteCloser, writers)
	outputs := make([]bytes.Buffer, writers)
	t.Cleanup(func() {
		cancel()
		for _, cmd := range commands {
			if cmd != nil && cmd.ProcessState == nil {
				_ = cmd.Wait()
			}
		}
	})
	for i := range commands {
		cmd := exec.CommandContext(ctx, executable, "-test.run=^TestCrossProcessPublication$")
		cmd.Env = append(os.Environ(), helperRoot+"="+root, helperWriter+"="+strconv.Itoa(i))
		cmd.Stdout, cmd.Stderr = &outputs[i], &outputs[i]
		gate, err := cmd.StdinPipe()
		if err != nil {
			t.Fatal(err)
		}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		commands[i], gates[i] = cmd, gate
	}
	for _, gate := range gates {
		if _, err := gate.Write([]byte{1}); err != nil {
			t.Fatal(err)
		}
		if err := gate.Close(); err != nil {
			t.Fatal(err)
		}
	}
	winner := -1
	for i, cmd := range commands {
		err := cmd.Wait()
		if err == nil {
			if winner != -1 {
				t.Fatal("multiple processes published the same key")
			}
			winner = i
			continue
		}
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != 3 {
			t.Fatalf("writer %d: %v: %s", i, err, outputs[i].String())
		}
	}
	if winner == -1 {
		t.Fatal("no process published")
	}
	if got := readBlob(t, s, "abcd"); !bytes.Equal(got, bytes.Repeat([]byte{byte(winner)}, size)) {
		t.Fatal("published content was incomplete, mixed, or overwritten")
	}
	assertNoPending(t, root)
}

func TestRelativeRootRemainsBoundToConstructorDirectory(t *testing.T) {
	base := t.TempDir()
	t.Chdir(base)
	s, err := NewFilesystem("blobs")
	if err != nil {
		t.Fatal(err)
	}
	other := t.TempDir()
	t.Chdir(other)
	assertRoundTrip(t, s, "abcd")
	if _, err := os.Stat(filepath.Join(other, "blobs")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("relative root followed changed working directory: %v", err)
	}
	assertNoPending(t, filepath.Join(base, "blobs"))
}
