package journal

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"
)

// ErrLeaseHeld reports a live owner of the root. A crashed owner cannot cause it:
// the lease is an OS file lock, which the kernel releases when the owner dies, so
// reopening after a crash needs no reclaim step.
var ErrLeaseHeld = errors.New("journal: root is held by a live owner")

// LeaseOwner is diagnostic identity recorded by the owner. It never decides
// exclusion, which rests entirely on the OS lock.
type LeaseOwner struct {
	PID     int       `json:"pid"`
	Host    string    `json:"host"`
	Started time.Time `json:"started"`
}

// LeaseError is the machine-checkable refusal returned when a live owner holds a
// root. It matches ErrLeaseHeld. Owner may be zero if the holder has not yet
// finished recording itself.
type LeaseError struct {
	Root  string
	Owner LeaseOwner
}

func (e *LeaseError) Error() string {
	return fmt.Sprintf("journal: root %s is held by a live owner (pid %d on %q since %s)", e.Root, e.Owner.PID, e.Owner.Host, e.Owner.Started.Format(time.RFC3339))
}

// Is makes errors.Is(err, ErrLeaseHeld) hold for this type.
func (e *LeaseError) Is(target error) bool { return target == ErrLeaseHeld }

type lease struct{ file *os.File }

// acquireLease takes the exclusive lock, then records this process as owner. The
// lease file is deliberately never deleted: unlinking a locked file would let two
// processes lock different inodes of "the same" lease.
func acquireLease(path, root string) (*lease, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	locked, err := tryLock(f)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("journal: lease lock: %w", err), f.Close())
	}
	if !locked {
		owner := readOwner(path)
		return nil, errors.Join(&LeaseError{Root: root, Owner: owner}, f.Close())
	}
	if err := recordOwner(f); err != nil {
		return nil, errors.Join(err, unlock(f), f.Close())
	}
	return &lease{file: f}, nil
}

func recordOwner(f *os.File) error {
	host, _ := os.Hostname()
	data, err := json.Marshal(LeaseOwner{PID: os.Getpid(), Host: host, Started: time.Now().UTC()})
	if err != nil {
		return err
	}
	if err := f.Truncate(0); err != nil {
		return err
	}
	_, err = f.WriteAt(data, 0)
	return err
}

func readOwner(path string) LeaseOwner {
	var owner LeaseOwner
	if data, err := os.ReadFile(path); err == nil {
		_ = json.Unmarshal(data, &owner)
	}
	return owner
}

func (l *lease) release() error {
	err := unlock(l.file)
	closeErr := l.file.Close()
	if errors.Is(closeErr, os.ErrClosed) {
		return nil
	}
	return errors.Join(err, closeErr)
}
