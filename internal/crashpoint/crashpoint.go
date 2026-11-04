// Package crashpoint names the write-stage boundaries that crash qualification
// kills a process at. Production code only ever calls Hit, which is a single
// atomic load when no hook is armed; nothing here reads the environment.
package crashpoint

import (
	"os"
	"sync/atomic"
)

// EnvVar is the variable helper processes conventionally read to pick a boundary.
const EnvVar = "ARTIFACT_SERVICES_CRASH_AT"

// Named write-stage boundaries, in pipeline order.
const (
	StorageTempSynced   = "storage.temp_synced"
	StorageLinked       = "storage.linked"
	CASSpooled          = "cas.spooled"
	CASPublished        = "cas.published"
	ArtifactsVerified   = "artifacts.verified"
	ArtifactsStored     = "artifacts.manifest_stored"
	ArtifactsRegistered = "artifacts.registered"
	JournalBeforeWrite  = "journal.before_write"
	JournalTornWrite    = "journal.torn_write"
	JournalBeforeSync   = "journal.before_sync"
	JournalCommitted    = "journal.committed"
	RunStarted          = "runs.started"
	RunOutputsPublished = "runs.outputs_published"
	RunCompleted        = "runs.completed"
	RunFinalized        = "runs.finalized"
)

// All lists every boundary so qualification can prove it covered each one.
var All = []string{
	StorageTempSynced, StorageLinked, CASSpooled, CASPublished,
	ArtifactsVerified, ArtifactsStored, ArtifactsRegistered,
	JournalBeforeWrite, JournalTornWrite, JournalBeforeSync, JournalCommitted,
	RunStarted, RunOutputsPublished, RunCompleted, RunFinalized,
}

type target struct {
	name string
	nth  int64
	hits atomic.Int64
}

var (
	hook  atomic.Pointer[func(string)]
	armed atomic.Pointer[target]
)

// Hit reports reaching a boundary.
func Hit(name string) {
	if f := hook.Load(); f != nil {
		(*f)(name)
	}
}

// Armed reports whether the next Hit of this boundary is the armed kill. It lets code
// do extra work (writing half a frame) only when that work will be observed.
func Armed(name string) bool {
	t := armed.Load()
	return t != nil && t.name == name && t.hits.Load()+1 == t.nth
}

// Set installs a hook for tests, returning a function that removes it.
func Set(f func(string)) (restore func()) {
	hook.Store(&f)
	return func() { hook.Store(nil) }
}

// KillOnHit arms the nth Hit of the named boundary, counting from one, to terminate
// this process without any cleanup, the in-process equivalent of kill -9.
func KillOnHit(name string, nth int64) {
	t := &target{name: name, nth: nth}
	armed.Store(t)
	Set(func(reached string) {
		if reached != name || t.hits.Add(1) != nth {
			return
		}
		if self, err := os.FindProcess(os.Getpid()); err == nil {
			_ = self.Kill()
		}
		select {} // Kill can be asynchronous; never run past the boundary.
	})
}
