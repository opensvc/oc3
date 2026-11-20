package cdb

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

// TestNoExecOnTheOperaterDirectly checks no function of the package executes
// a statement on the DBOperater of a DB (oDb.DB.ExecContext): out of a
// transaction, that is the connection pool, where a deadlock fails the
// statement at once. The statements go through DB.ExecContext, the executor
// retrying on a deadlock, which is the only function allowed to call the
// operater.
func TestNoExecOnTheOperaterDirectly(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			if name == "db.go" && fn.Name.Name == "ExecContext" {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != "ExecContext" {
					return true
				}
				if operater, ok := sel.X.(*ast.SelectorExpr); ok && operater.Sel.Name == "DB" {
					t.Errorf("%s: %s executes on the operater directly, out of the deadlock retry: use oDb.ExecContext",
						fset.Position(call.Pos()), fn.Name.Name)
				}
				return true
			})
		}
	}
}
