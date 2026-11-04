//go:build windows

package durable

import "os"

// SyncDir is a no-op on Windows: directory handles cannot be flushed, and NTFS
// journals namespace changes. Published forces that journal with syncLink.
func SyncDir(string) error { return nil }

// syncLink flushes through the new name. FlushFileBuffers commits the NTFS log up
// to this file's latest metadata change, which includes the link creation.
func syncLink(path string) error { return syncOpen(path, os.O_RDWR) }
