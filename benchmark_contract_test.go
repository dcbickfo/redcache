package redcache_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"testing"
)

func TestBenchmarksDoNotFailNowInsideRunParallel(t *testing.T) {
	t.Parallel()

	files, err := filepath.Glob("*_test.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		t.Run(file, func(t *testing.T) {
			t.Parallel()
			assertNoFailNowInsideRunParallel(t, file)
		})
	}
}

func assertNoFailNowInsideRunParallel(t *testing.T, filename string) {
	t.Helper()

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, filename, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", filename, err)
	}

	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || !isSelectorNamed(call.Fun, "RunParallel") {
			return true
		}
		for _, arg := range call.Args {
			lit, ok := arg.(*ast.FuncLit)
			if !ok {
				continue
			}
			ast.Inspect(lit.Body, func(n ast.Node) bool {
				inner, ok := n.(*ast.CallExpr)
				if !ok || !isFatalSelector(inner.Fun) {
					return true
				}
				t.Errorf("%s calls FailNow/Fatal inside RunParallel worker", fset.Position(inner.Pos()))
				return true
			})
		}
		return true
	})
}

func isSelectorNamed(expr ast.Expr, name string) bool {
	sel, ok := expr.(*ast.SelectorExpr)
	return ok && sel.Sel.Name == name
}

func isFatalSelector(expr ast.Expr) bool {
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	switch sel.Sel.Name {
	case "Fatal", "Fatalf", "FailNow":
		return true
	default:
		return false
	}
}
