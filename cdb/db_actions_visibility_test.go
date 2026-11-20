package cdb

import (
	"context"
	"fmt"
	"testing"
)

// TestActionsVisibility checks the actions a user sees: those on a node of
// their apps and those of a service of their apps, wherever it runs.
func TestActionsVisibility(t *testing.T) {
	db := testMariaDB(t,
		"CREATE TABLE action_queue (id int, node_id char(36), svc_id char(36) DEFAULT '', command varchar(64))",
		"CREATE TABLE nodes (node_id char(36), nodename varchar(64), app varchar(64), serial varchar(64))",
		"CREATE TABLE services (svc_id char(36), svcname varchar(64), svc_app varchar(64), svc_comment varchar(64))",
		"CREATE TABLE apps (id int, app varchar(64))",
		"CREATE TABLE apps_responsibles (app_id int, group_id int)",
		"CREATE TABLE auth_group (id int, role varchar(64))",
		"INSERT INTO apps VALUES (1, 'A'), (2, 'B')",
		"INSERT INTO apps_responsibles VALUES (1, 10), (2, 20)",
		"INSERT INTO auth_group VALUES (10, 'teamA'), (20, 'teamB')",
		"INSERT INTO nodes VALUES ('nA', 'nodeA', 'A', 'SA'), ('nB', 'nodeB', 'B', 'SB')",
		"INSERT INTO services VALUES ('sA', 'svcA', 'A', 'cA'), ('sB', 'svcB', 'B', 'cB')",
		// 1 service A on node B, 2 service B on node A, 3 service B on node B
		"INSERT INTO action_queue VALUES (1, 'nB', 'sA', 'x'), (2, 'nA', 'sB', 'x'), (3, 'nB', 'sB', 'x')",
	)
	odb := testDashboardDB(db)
	rows, err := odb.GetActions(context.Background(), ListParams{
		Groups:      []string{"teamA"},
		SelectExprs: []string{"action_queue.id", "nodes.nodename", "services.svcname"},
		Props:       []string{"id", "nodes.nodename", "services.svcname"},
		OrderBy:     []string{"action_queue.id"},
	})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, r := range rows {
		got = append(got, fmt.Sprintf("%v %v %v", r["id"], r["nodes.nodename"], r["services.svcname"]))
	}
	if want := "[1 nodeB svcA 2 nodeA svcB]"; fmt.Sprint(got) != want {
		t.Errorf("team A: %v\nwant %s", got, want)
	}
}
