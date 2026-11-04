package integration_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestDocExamplesCompile compiles every complete ```go block (one carrying a
// package clause) from docs/ in a separate consumer module, so guide examples
// cannot silently rot against the public API. Fragments without a package
// clause are illustrative and skipped.
func TestDocExamplesCompile(t *testing.T) {
	root, err := filepath.Abs(filepath.Join(".."))
	if err != nil {
		t.Fatal(err)
	}
	docs, err := filepath.Glob(filepath.Join(root, "docs", "*.md"))
	if err != nil || len(docs) == 0 {
		t.Fatal("docs not found", err)
	}
	compiled := 0
	for _, doc := range docs {
		data, err := os.ReadFile(doc)
		if err != nil {
			t.Fatal(err)
		}
		for i, block := range extractGoBlocks(string(data)) {
			if !strings.HasPrefix(strings.TrimSpace(block), "package ") {
				continue
			}
			compiled++
			dir := t.TempDir()
			goMod := fmt.Sprintf("module example.com/docexample\n\ngo 1.27\n\nrequire github.com/craggy-io/artifact-services v0.0.0\n\nreplace github.com/craggy-io/artifact-services => %s\n", filepath.ToSlash(root))
			if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte(goMod), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "example.go"), []byte(block), 0600); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command("go", "build", "./...")
			cmd.Dir = dir
			cmd.Env = append(os.Environ(), "GOFLAGS=-mod=mod", "GOPROXY=off")
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("%s block %d does not compile: %v\n%s", filepath.Base(doc), i, err, out)
			}
		}
	}
	if compiled == 0 {
		t.Fatal("no complete doc examples found")
	}
	t.Logf("compiled %d complete doc examples", compiled)
}

func extractGoBlocks(md string) []string {
	var blocks []string
	rest := md
	for {
		start := strings.Index(rest, "```go")
		if start < 0 {
			return blocks
		}
		rest = rest[start+len("```go"):]
		end := strings.Index(rest, "```")
		if end < 0 {
			return blocks
		}
		blocks = append(blocks, rest[:end])
		rest = rest[end+len("```"):]
	}
}
