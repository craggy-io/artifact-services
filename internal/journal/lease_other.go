//go:build !unix && !windows

package journal

import (
	"errors"
	"os"
)

func tryLock(*os.File) (bool, error) {
	return false, errors.New("journal: no OS file lock on this platform")
}
func unlock(*os.File) error { return nil }
