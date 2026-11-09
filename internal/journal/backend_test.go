package journal

import (
	"context"
	"testing"

	"github.com/craggy-io/artifact-services/backend"
	"github.com/craggy-io/artifact-services/backend/backendtest"
)

func TestFileLogConformance(t *testing.T) {
	backendtest.Run(t, func(t *testing.T) backend.Opener {
		root := t.TempDir()
		return func(_ context.Context, config []byte) (backend.Log, error) {
			return openFile(root, Options{Relaxed: true}, config)
		}
	}, false)
}
