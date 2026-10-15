package serverhandlers

import (
	"slices"
	"testing"

	"github.com/opensvc/oc3/server"
)

// A groupby names a prop of the list or, as table.column, a column of a joined
// table, as the historical forms group the targets of a node by array.
func TestBuildGroupByJoinedColumn(t *testing.T) {
	mapping := propsMapping["node_target"]
	gb := server.InQueryGroupby("stor_array.id,hba_id")
	got, err := buildGroupBy(&gb, mapping)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, []string{"stor_array.id", "stor_zone.hba_id"}) {
		t.Errorf("got %v", got)
	}
	for _, bad := range []string{"diskinfo.disk_group", "stor_array.nope", "nope"} {
		gb := server.InQueryGroupby(bad)
		if _, err := buildGroupBy(&gb, mapping); err == nil {
			t.Errorf("%s: expected an error", bad)
		}
	}
}
