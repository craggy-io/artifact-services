//go:build windows

package journal

import (
	"errors"
	"os"
	"syscall"
	"unsafe"
)

var (
	kernel32         = syscall.NewLazyDLL("kernel32.dll")
	procLockFileEx   = kernel32.NewProc("LockFileEx")
	procUnlockFileEx = kernel32.NewProc("UnlockFileEx")
)

const (
	lockExclusive      = 0x00000002
	lockFailImmediate  = 0x00000001
	errorLockViolation = syscall.Errno(33)
	errorIOPending     = syscall.Errno(997)
	// The lock covers one byte far past the owner record, because Windows locks
	// are mandatory and would otherwise stop a refused opener reading who owns it.
	lockOffsetHigh = 0x7fff0000
)

// A handle's lock conflicts with every other handle, in this process or not, and
// the kernel releases it when the owning process dies.
func tryLock(f *os.File) (bool, error) {
	overlapped := syscall.Overlapped{OffsetHigh: lockOffsetHigh}
	r, _, err := procLockFileEx.Call(f.Fd(), lockExclusive|lockFailImmediate, 0, 1, 0, uintptr(unsafe.Pointer(&overlapped)))
	if r != 0 {
		return true, nil
	}
	if errors.Is(err, errorLockViolation) || errors.Is(err, errorIOPending) {
		return false, nil
	}
	return false, err
}

func unlock(f *os.File) error {
	overlapped := syscall.Overlapped{OffsetHigh: lockOffsetHigh}
	r, _, err := procUnlockFileEx.Call(f.Fd(), 0, 1, 0, uintptr(unsafe.Pointer(&overlapped)))
	if r != 0 {
		return nil
	}
	return err
}
