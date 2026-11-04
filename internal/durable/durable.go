// Package durable makes file publication survive OS crash and power loss on a
// local filesystem. Content syncing is the caller's job; this package syncs the
// namespace change that made the content visible.
package durable

import (
	"os"
	"path/filepath"
)

// Published flushes the namespace change that made path visible, plus the chain
// of parent directories up to and including stop. An empty stop syncs only the
// immediate parent. Call it after creating a file or hard link, and before
// reporting that publication is durable.
func Published(path, stop string) error {
	if err := syncLink(path); err != nil {
		return err
	}
	dir := filepath.Dir(path)
	for {
		if err := SyncDir(dir); err != nil {
			return err
		}
		parent := filepath.Dir(dir)
		if stop == "" || dir == stop || parent == dir {
			return nil
		}
		dir = parent
	}
}

func syncOpen(path string, flag int) error {
	f, err := os.OpenFile(path, flag, 0)
	if err != nil {
		return err
	}
	syncErr := f.Sync()
	closeErr := f.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}
