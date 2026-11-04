package integration_test

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// publicPackages is the stable surface, in the order of the golden file. Anything
// under internal/ is free to change.
var publicPackages = []string{
	"storage", "cas", "artifacts", "objects", "lineage", "assertions", "provenance", "observability",
	"adapters/stack", "adapters/runs", "adapters/typed", "adapters/spool", "adapters/treeio", "adapters/gc", "adapters/testkit",
}

const updateGoldenEnv = "ARTIFACT_SERVICES_UPDATE_GOLDEN"

func exported(name string) bool { return ast.IsExported(name) }

func render(fset *token.FileSet, node any) string {
	var buffer bytes.Buffer
	if err := (&printer.Config{Mode: printer.RawFormat}).Fprint(&buffer, fset, node); err != nil {
		panic(err)
	}
	return strings.Join(strings.Fields(buffer.String()), " ")
}

// exportedFields keeps only what callers can name, so private fields can change freely.
func exportedFields(fset *token.FileSet, list *ast.FieldList) []string {
	var lines []string
	for _, field := range list.List {
		if len(field.Names) == 0 {
			if expr := render(fset, field.Type); exported(strings.TrimLeft(expr[strings.LastIndex(expr, ".")+1:], "*")) {
				lines = append(lines, expr)
			}
			continue
		}
		for _, name := range field.Names {
			if exported(name.Name) {
				lines = append(lines, name.Name+" "+render(fset, field.Type))
			}
		}
	}
	return lines
}

func describeType(fset *token.FileSet, prefix string, spec *ast.TypeSpec) []string {
	head := prefix + " type " + spec.Name.Name
	if spec.TypeParams != nil {
		var params []string
		for _, param := range spec.TypeParams.List {
			var names []string
			for _, name := range param.Names {
				names = append(names, name.Name)
			}
			params = append(params, strings.Join(names, ", ")+" "+render(fset, param.Type))
		}
		head += "[" + strings.Join(params, ", ") + "]"
	}
	var out []string
	switch t := spec.Type.(type) {
	case *ast.StructType:
		out = append(out, head+" struct")
		for _, line := range exportedFields(fset, t.Fields) {
			out = append(out, head+" field "+line)
		}
	case *ast.InterfaceType:
		out = append(out, head+" interface")
		for _, method := range t.Methods.List {
			line := render(fset, method.Type)
			if len(method.Names) > 0 {
				// A method's type renders as "func(...)"; the name replaces "func".
				line = method.Names[0].Name + strings.TrimPrefix(line, "func")
			}
			out = append(out, head+" method "+line)
		}
	default:
		alias := " = "
		if !spec.Assign.IsValid() {
			alias = " "
		}
		out = append(out, head+alias+render(fset, spec.Type))
	}
	return out
}

func describeFunc(fset *token.FileSet, prefix string, decl *ast.FuncDecl) (string, bool) {
	if !exported(decl.Name.Name) {
		return "", false
	}
	if decl.Recv != nil {
		receiver := render(fset, decl.Recv.List[0].Type)
		base, _, _ := strings.Cut(strings.TrimPrefix(receiver, "*"), "[")
		if !exported(base) {
			return "", false
		}
		decl = &ast.FuncDecl{Recv: decl.Recv, Name: decl.Name, Type: decl.Type}
	} else {
		decl = &ast.FuncDecl{Name: decl.Name, Type: decl.Type}
	}
	return prefix + " " + render(fset, decl), true
}

func exportedAPI(t *testing.T) string {
	t.Helper()
	root := filepath.Join("..")
	var lines []string
	for _, pkg := range publicPackages {
		entries, err := os.ReadDir(filepath.Join(root, filepath.FromSlash(pkg)))
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			name := entry.Name()
			if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, filepath.Join(root, filepath.FromSlash(pkg), name), nil, parser.SkipObjectResolution)
			if err != nil {
				t.Fatal(err)
			}
			for _, decl := range file.Decls {
				switch d := decl.(type) {
				case *ast.FuncDecl:
					if line, ok := describeFunc(fset, pkg, d); ok {
						lines = append(lines, line)
					}
				case *ast.GenDecl:
					for _, spec := range d.Specs {
						switch s := spec.(type) {
						case *ast.TypeSpec:
							if exported(s.Name.Name) {
								lines = append(lines, describeType(fset, pkg, s)...)
							}
						case *ast.ValueSpec:
							for _, value := range s.Names {
								if exported(value.Name) {
									lines = append(lines, fmt.Sprintf("%s %s %s", pkg, strings.ToLower(d.Tok.String()), value.Name))
								}
							}
						}
					}
				}
			}
		}
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n") + "\n"
}

// TestExportedAPIMatchesGolden makes every change to the exported surface a reviewed
// change to api.golden.txt. See docs/stability.md for what that change then obliges.
func TestExportedAPIMatchesGolden(t *testing.T) {
	path := filepath.Join("..", "api.golden.txt")
	current := exportedAPI(t)
	if os.Getenv(updateGoldenEnv) != "" {
		if err := os.WriteFile(path, []byte(current), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	golden, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want := strings.ReplaceAll(string(golden), "\r\n", "\n")
	if current == want {
		return
	}
	have, expected := map[string]bool{}, map[string]bool{}
	for _, line := range strings.Split(current, "\n") {
		have[line] = true
	}
	for _, line := range strings.Split(want, "\n") {
		expected[line] = true
		if !have[line] {
			t.Errorf("removed: %s", line)
		}
	}
	for _, line := range strings.Split(current, "\n") {
		if !expected[line] {
			t.Errorf("added: %s", line)
		}
	}
	t.Errorf("exported API changed; if intended, rerun with %s=1 and review the diff", updateGoldenEnv)
}
