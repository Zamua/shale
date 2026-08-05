package cluster

// SOURCE-SHAPE pin for the serving-marker publish. The behavioral pins in
// serving_marker_publish_internal_test.go cover the routes to serving that
// EXIST; this one covers the route that does not exist yet, by asserting the
// shape that makes "mounted here but never marked" unwritable:
//
//   - the marker write funnels through the mount table's publish hook, so no
//     mount path can publish its own (or forget to);
//   - the mount helpers that do NOT publish are only ever handed a replica0()
//     position, i.e. the SOLE (R=1) layout, which has no marker protocol;
//   - the mount map itself is only written by those helpers, so the list above
//     is closed.
//
// A failure here is not necessarily a bug: it means the code moved out of the
// shape, and whoever moved it has to say why. Renaming is free (update the
// expected sets); routing a NEW replica-layout mount around mountServing is
// what this refuses to let pass silently.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestServingPublish_MarkerWriteFunnelsThroughTheMountTable(t *testing.T) {
	funcs := productionFuncs(t)

	for _, tc := range []struct {
		sel  string // the selector whose uses are constrained
		want []string
		why  string
	}{
		{
			sel:  "WriteServingMarker",
			want: []string{"Cluster.writeServingMarker"},
			why:  "the factory's marker write has one wrapper so a failed write cannot be swallowed at a call site",
		},
		{
			sel:  "writeServingMarker",
			want: []string{"Cluster.publishServingMarker"},
			why:  "the wrapper is reached only through the publisher, which resolves the epoch first",
		},
		{
			sel:  "publishServingMarker",
			want: []string{"mountTable.init"},
			why:  "the publisher is wired into the table once and called by nothing else",
		},
		{
			sel:  "publish",
			want: []string{"mountTable.init", "mountTable.publishServing"},
			why:  "the table's publish hook is set at init and invoked only behind publishServing (which owns the lock discipline)",
		},
		{
			sel:  "publishServing",
			want: []string{"mountTable.finishStuckGainer", "mountTable.mountServing"},
			why:  "publishing is a property of the two transitions that make a position locally serving",
		},
	} {
		got := usesOf(funcs, tc.sel)
		if !slices.Equal(got, tc.want) {
			t.Errorf("%s is used in %v, want %v\n(%s)", tc.sel, got, tc.want, tc.why)
		}
	}
}

// TestServingPublish_NonPublishingMountsAreSoleLayoutOnly: mount, mountPair and
// mountUnlessClosed install a mount WITHOUT publishing a marker or recording an
// open epoch, which is correct only for the SOLE (R=1) layout. Every call site
// spells that layout out by passing replica0(gu), so a replica-layout position
// reaching one of them is visible in the source rather than only in a
// predecessor that stays Draining on another node.
func TestServingPublish_NonPublishingMountsAreSoleLayoutOnly(t *testing.T) {
	// The position argument index per helper.
	soleOnly := map[string][]int{
		"mount":             {0},
		"mountPair":         {0, 2},
		"mountUnlessClosed": {0},
	}
	calls := 0
	forEachProductionNode(t, productionFuncs(t), func(fn string, n ast.Node) {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return
		}
		idx, constrained := soleOnly[sel.Sel.Name]
		if !constrained || !isMountTableReceiver(sel.X) {
			return
		}
		calls++
		for _, i := range idx {
			if i >= len(call.Args) || !isCallOf(call.Args[i], "replica0") {
				t.Errorf("%s: %s arg %d is not replica0(...): this helper publishes no serving marker, "+
					"so a replica-layout position must go through mountServing instead", fn, sel.Sel.Name, i)
			}
		}
	})
	if calls == 0 {
		t.Fatalf("found no calls to the non-publishing mount helpers; the scan is not seeing the package")
	}
}

// TestServingPublish_MountMapHasOneWriter closes the list above: a helper that
// wrote t.mounts directly would install a mount none of the other pins can see.
func TestServingPublish_MountMapHasOneWriter(t *testing.T) {
	// mountLocked is the funnel; mountUndecorated is the declared white-box seam
	// that deliberately skips the decorator.
	allowed := []string{"mountTable.mountLocked", "mountTable.mountUndecorated"}
	var got []string
	forEachProductionNode(t, productionFuncs(t), func(fn string, n ast.Node) {
		assign, ok := n.(*ast.AssignStmt)
		if !ok {
			return
		}
		for _, lhs := range assign.Lhs {
			idx, ok := lhs.(*ast.IndexExpr)
			if !ok || !isMountTableReceiver(idx.X) {
				continue
			}
			if !slices.Contains(got, fn) {
				got = append(got, fn)
			}
		}
	})
	slices.Sort(got)
	if !slices.Equal(got, allowed) {
		t.Errorf("the mount map is written in %v, want %v: a new writer bypasses the serving transition", got, allowed)
	}
}

// -- source-shape helpers ---------------------------------------------------

// productionFuncs parses the package's non-test files and returns every
// function declaration keyed by "ReceiverType.Name" (bare name when there is no
// receiver). The test binary runs in the package directory.
func productionFuncs(t *testing.T) map[string]*ast.FuncDecl {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}
	fset := token.NewFileSet()
	out := make(map[string]*ast.FuncDecl)
	files := 0
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(".", name), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		files++
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			out[qualifiedFuncName(fd)] = fd
		}
	}
	if files == 0 {
		t.Fatalf("no production files parsed; the scan would pass vacuously")
	}
	return out
}

// qualifiedFuncName renders fd as "ReceiverType.Name", pointer receivers
// flattened to the bare type.
func qualifiedFuncName(fd *ast.FuncDecl) string {
	if fd.Recv == nil || len(fd.Recv.List) == 0 {
		return fd.Name.Name
	}
	expr := fd.Recv.List[0].Type
	if star, ok := expr.(*ast.StarExpr); ok {
		expr = star.X
	}
	if id, ok := expr.(*ast.Ident); ok {
		return id.Name + "." + fd.Name.Name
	}
	return fd.Name.Name
}

// forEachProductionNode visits every node of every production function body,
// passing the enclosing function's qualified name.
func forEachProductionNode(t *testing.T, funcs map[string]*ast.FuncDecl, visit func(fn string, n ast.Node)) {
	t.Helper()
	for name, fd := range funcs {
		if fd.Body == nil {
			continue
		}
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			if n != nil {
				visit(name, n)
			}
			return true
		})
	}
}

// usesOf returns, sorted, the production functions whose body mentions the
// selector sel (a call OR a method value, so wiring a hook counts as a use).
func usesOf(funcs map[string]*ast.FuncDecl, sel string) []string {
	var out []string
	for name, fd := range funcs {
		if fd.Body == nil {
			continue
		}
		found := false
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			if s, ok := n.(*ast.SelectorExpr); ok && s.Sel.Name == sel {
				found = true
			}
			return !found
		})
		if found {
			out = append(out, name)
		}
	}
	slices.Sort(out)
	return out
}

// isMountTableReceiver reports whether expr addresses the mount table: a
// holder's "mounts" field (c.mounts.helper, t.mounts[ru]) or the table's own
// receiver (t.helper, from inside mounttable.go).
func isMountTableReceiver(expr ast.Expr) bool {
	switch x := expr.(type) {
	case *ast.SelectorExpr:
		return x.Sel.Name == "mounts"
	case *ast.Ident:
		return x.Name == "t"
	}
	return false
}

// isCallOf reports whether expr is a call to the named function.
func isCallOf(expr ast.Expr, name string) bool {
	call, ok := expr.(*ast.CallExpr)
	if !ok {
		return false
	}
	id, ok := call.Fun.(*ast.Ident)
	return ok && id.Name == name
}
