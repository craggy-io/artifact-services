package postgres_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/craggy-io/artifact-services/adapters/postgres"
	"github.com/craggy-io/artifact-services/adapters/stack"
	"github.com/craggy-io/artifact-services/adapters/stack/stacktest"
	"github.com/craggy-io/artifact-services/backend"
	"github.com/craggy-io/artifact-services/backend/backendtest"
	"github.com/craggy-io/artifact-services/storage"
)

// DSNVariable names the database the tests use. They fail, never skip, without it:
// CI runs PostgreSQL as a service, and locally `docker compose up` in craggy-cloud.
const DSNVariable = "ARTIFACT_SERVICES_POSTGRES_DSN"

var prefixes atomic.Int64

// pool opens a pool for one simulated process.
func pool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv(DSNVariable)
	if dsn == "" {
		t.Fatalf("%s is not set; these tests need PostgreSQL", DSNVariable)
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	config.MaxConns = 8
	p, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

func newBackend(t *testing.T, prefix string) *postgres.Backend {
	t.Helper()
	b, err := postgres.New(context.Background(), pool(t), postgres.Options{Prefix: prefix})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	return b
}

// uniquePrefix isolates each test's roots within the shared database.
func uniquePrefix(t *testing.T) string {
	return fmt.Sprintf("test/%d/%d/", os.Getpid(), prefixes.Add(1))
}

func TestConformance(t *testing.T) {
	backendtest.Run(t, func(t *testing.T) backend.Opener {
		return newBackend(t, uniquePrefix(t)).Opener("root")
	}, true)
}

// TestTwoProcessesShareOneStack runs the stack suite with every stack on its own
// pool and backend, as separate Cloud Run instances would be, over one database
// and one blob directory.
func TestTwoProcessesShareOneStack(t *testing.T) {
	stacktest.Run(t, func(t *testing.T) func() (*stack.Stack, error) {
		prefix, blobs, dir := uniquePrefix(t), t.TempDir(), t.TempDir()
		var n atomic.Int64
		return func() (*stack.Stack, error) {
			shared, err := storage.NewFilesystem(blobs)
			if err != nil {
				return nil, err
			}
			root := filepath.Join(dir, fmt.Sprint(n.Add(1)))
			return stack.Open(stack.Options{Root: root, Blobs: shared, Journals: newBackend(t, prefix).Journals()})
		}
	}, true)
}

func TestMigrateIsConcurrentSafe(t *testing.T) {
	errs := make(chan error, 4)
	for i := 0; i < 4; i++ {
		p := pool(t)
		go func() {
			_, err := postgres.New(context.Background(), p, postgres.Options{})
			errs <- err
		}()
	}
	for i := 0; i < 4; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
}
