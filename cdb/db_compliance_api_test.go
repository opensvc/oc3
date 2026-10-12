package cdb

import "testing"

// TestCompStatusGroupsOrder checks the counts of a group and their order: the
// least compliant first, then the largest, then by name, a group without any run
// last.
func TestCompStatusGroupsOrder(t *testing.T) {
	group := func(name string, ok, nok, na, obs int64) CompStatusGroup {
		g := CompStatusGroup{ID: name, Name: name, CompStatusCounts: CompStatusCounts{OK: ok, NOK: nok, NA: na, Obs: obs}}
		g.complete()
		return g
	}
	groups := []CompStatusGroup{
		group("empty", 0, 0, 0, 0),
		group("b", 3, 1, 0, 0),
		group("a", 1, 0, 1, 0),
		group("c", 6, 2, 0, 0),
		group("d", 0, 1, 0, 1),
	}
	if g := groups[1]; g.Total != 4 || g.Pct == nil || *g.Pct != 75 {
		t.Fatalf("counts of b: %+v", g.CompStatusCounts)
	}
	if groups[0].Pct != nil {
		t.Fatalf("a group without runs has no percent: %v", *groups[0].Pct)
	}
	sortCompStatusGroups(groups)
	want := []string{"d", "c", "b", "a", "empty"}
	for i, name := range want {
		if groups[i].Name != name {
			t.Fatalf("order: got %v at %d, want %v", groups[i].Name, i, name)
		}
	}
}
