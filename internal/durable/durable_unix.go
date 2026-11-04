//go:build !windows

package durable

import (
	"errors"
	"os"
	"syscall"
)

// SyncDir fsyncs a directory so entries created in it survive power loss.
func SyncDir(dir string) error {
	err := syncOpen(dir, os.O_RDONLY)
	// Some filesystems reject fsync on directories; they offer no stronger option.
	if errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ENOTSUP) {
		return nil
	}
	return err
}

// The directory fsync in Published covers the new name on POSIX.
func syncLink(string) error { return nil }
