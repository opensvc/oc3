package cdb

import (
	"strings"
	"testing"

	"github.com/opensvc/oc3/schema"
)

// TestBuildServicesInstancesQueryJoinsActionErrors checks that the instances query
// LEFT JOINs b_action_errors on the service and the node, and that a filter on its
// count needs no declared relation to that table.
func TestBuildServicesInstancesQueryJoinsActionErrors(t *testing.T) {
	filters := []ColumnFilter{{
		Col:  schema.BActionErrorsErr,
		Expr: "COALESCE(b_action_errors.err, 0) > ?",
		Args: []any{0},
	}}
	query, args, err := buildServicesInstancesQuery(nil, true, []string{"COALESCE(b_action_errors.err, 0)"}, filters)
	if err != nil {
		t.Fatal(err)
	}
	join := "LEFT JOIN b_action_errors ON b_action_errors.svc_id = svcmon.svc_id AND b_action_errors.node_id = svcmon.node_id"
	if strings.Count(query, join) != 1 {
		t.Errorf("want the join once, got:\n%s", query)
	}
	// The join comes before the conditions, after the joins it relies on.
	if strings.Index(query, join) > strings.Index(query, "WHERE") {
		t.Errorf("join after WHERE:\n%s", query)
	}
	if len(args) != 2 {
		t.Errorf("args: got %v", args)
	}
}
