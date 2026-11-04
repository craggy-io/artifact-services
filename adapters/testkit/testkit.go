// Package testkit builds a full service stack for tests, in a temporary directory,
// with the correct wiring and close order. It has no runtime role.
package testkit

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/craggy-io/artifact-services/adapters/runs"
	"github.com/craggy-io/artifact-services/adapters/stack"
	"github.com/craggy-io/artifact-services/artifacts"
	"github.com/craggy-io/artifact-services/cas"
)

// Stack is an open stack that closes itself when the test ends. Options can be
// changed before Reopen to test what a later process would see.
type Stack struct {
	*stack.Stack
	T       testing.TB
	Ctx     context.Context
	Options stack.Options
}

// Options narrows the defaults. Root is ignored, and journals default to Relaxed
// because tests do not survive OS crashes and fsync per commit is slow.
type Options = stack.Options

// Open starts a stack under t.TempDir.
func Open(t testing.TB, options ...Options) *Stack {
	t.Helper()
	var o Options
	if len(options) > 0 {
		o = options[0]
	} else {
		o.Relaxed = true
	}
	o.Root = t.TempDir()
	s := &Stack{T: t, Ctx: context.Background(), Options: o}
	s.open()
	return s
}

func (s *Stack) open() {
	s.T.Helper()
	opened, err := stack.Open(s.Options)
	if err != nil {
		s.T.Fatal(err)
	}
	s.Stack = opened
	s.T.Cleanup(func() {
		if err := opened.Close(); err != nil {
			s.T.Errorf("close stack: %v", err)
		}
	})
}

// Reopen closes every service and opens new ones over the same root.
func (s *Stack) Reopen() {
	s.T.Helper()
	if err := s.Stack.Close(); err != nil {
		s.T.Fatal(err)
	}
	s.open()
}

// Runs returns a coordinator over this stack's current services.
func (s *Stack) Runs() *runs.Coordinator {
	s.T.Helper()
	c, err := runs.New(runs.Options{CAS: s.CAS, Artifacts: s.Artifacts, Lineage: s.Lineage, Assertions: s.Assertions})
	if err != nil {
		s.T.Fatal(err)
	}
	return c
}

// Session begins a session over a new coordinator. Ending it is the test's business,
// since an unfinished session is itself something tests assert on.
func (s *Stack) Session(actor string) *runs.Session {
	s.T.Helper()
	session, err := s.Runs().Begin(s.Ctx, runs.SessionRequest{Actor: actor})
	if err != nil {
		s.T.Fatal(err)
	}
	return session
}

// Put stores bytes and returns their digest and size.
func (s *Stack) Put(data string) (cas.ID, int64) {
	s.T.Helper()
	put, err := s.CAS.Put(s.Ctx, strings.NewReader(data))
	if err != nil {
		s.T.Fatal(err)
	}
	return put.ID, put.Size
}

// File publishes one regular file as an artifact.
func (s *Stack) File(path, data string) artifacts.ID {
	s.T.Helper()
	content, size := s.Put(data)
	id, err := s.Artifacts.Publish(s.Ctx, artifacts.Manifest{Version: 1, Entries: []artifacts.Entry{{Path: path, Content: content, Size: size}}})
	if err != nil {
		s.T.Fatal(err)
	}
	return id
}

// IDs returns a generator of predictable, distinct IDs, for tests that assert on
// them. Core mints random IDs only when a caller supplies none.
func IDs(prefix string) func() string {
	var next atomic.Int64
	return func() string { return fmt.Sprintf("%s-%d", prefix, next.Add(1)) }
}
