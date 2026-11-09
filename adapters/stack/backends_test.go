package stack_test

import (
	"testing"

	"github.com/craggy-io/artifact-services/adapters/stack"
	"github.com/craggy-io/artifact-services/adapters/stack/stacktest"
)

func TestLocalBackends(t *testing.T) {
	stacktest.Run(t, func(t *testing.T) func() (*stack.Stack, error) {
		root := t.TempDir()
		return func() (*stack.Stack, error) { return stack.Open(stack.Options{Root: root, Relaxed: true}) }
	}, false)
}
