package cdb

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// TestMaskJoinedColumns checks the columns of a joined node or service of
// another app read NULL, their identity aside, and the arguments follow.
func TestMaskJoinedColumns(t *testing.T) {
	exprs := []string{"action_queue.id", "nodes.nodename", "nodes.serial", "services.svcname", "services.svc_comment", "COUNT(*)"}
	if got, args := maskJoinedColumns(exprs, []string{"teamA"}, true, nodesMask, servicesMask); fmt.Sprint(got) != fmt.Sprint(exprs) || args != nil {
		t.Errorf("manager: %v %v", got, args)
	}
	got, args := maskJoinedColumns(exprs, []string{"teamA", "teamB"}, false, nodesMask, servicesMask)
	for i, want := range []string{"action_queue.id", "nodes.nodename", "IF(nodes.app IN (", "services.svcname", "IF(services.svc_app IN (", "COUNT(*)"} {
		if !strings.HasPrefix(got[i], want) {
			t.Errorf("%d: %q, want %q…", i, got[i], want)
		}
	}
	if n := strings.Count(strings.Join(got, ","), "?"); n != len(args) || len(args) != 4 {
		t.Errorf("%d placeholders, %d arguments", n, len(args))
	}
	if got, args := maskJoinedColumns([]string{"nodes.serial"}, nil, false, nodesMask); got[0] != "NULL" || args != nil {
		t.Errorf("no group: %v %v", got, args)
	}
}

// TestActionsJoinedColumnsMasked checks the node or the service of another app
// of an action a user sees shows its name only.
func TestActionsJoinedColumnsMasked(t *testing.T) {
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
		SelectExprs: []string{"action_queue.id", "nodes.nodename", "nodes.serial", "services.svcname", "services.svc_comment"},
		Props:       []string{"id", "nodes.nodename", "nodes.serial", "services.svcname", "services.svc_comment"},
		OrderBy:     []string{"action_queue.id"},
	})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, r := range rows {
		got = append(got, fmt.Sprintf("%v %v %v %v %v", r["id"], r["nodes.nodename"], r["nodes.serial"], r["services.svcname"], r["services.svc_comment"]))
	}
	if want := "[1 nodeB <nil> svcA cA 2 nodeA SA svcB <nil>]"; fmt.Sprint(got) != want {
		t.Errorf("team A: %v\nwant %s", got, want)
	}
}
