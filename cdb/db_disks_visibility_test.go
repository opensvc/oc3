package cdb

import (
	"context"
	"fmt"
	"testing"
)

// TestDisksVisibility checks a disk shared by nodes of two apps shows, to a
// user of one of them, its use by their nodes only; all of it to a manager.
func TestDisksVisibility(t *testing.T) {
	db := testMariaDB(t,
		"CREATE TABLE diskinfo (id int, disk_id varchar(120), disk_group varchar(64) DEFAULT '')",
		"CREATE TABLE svcdisks (id int, disk_id varchar(120), node_id char(36), svc_id char(36))",
		"CREATE TABLE nodes (node_id char(36), nodename varchar(64), app varchar(64))",
		"CREATE TABLE services (svc_id char(36), svcname varchar(64), svc_app varchar(64))",
		"CREATE TABLE apps (id int, app varchar(64))",
		"CREATE TABLE apps_responsibles (app_id int, group_id int)",
		"CREATE TABLE auth_group (id int, role varchar(64))",
		"INSERT INTO apps VALUES (1, 'A'), (2, 'B')",
		"INSERT INTO apps_responsibles VALUES (1, 10), (2, 20)",
		"INSERT INTO auth_group VALUES (10, 'teamA'), (20, 'teamB')",
		"INSERT INTO nodes VALUES ('nA', 'nodeA', 'A'), ('nB', 'nodeB', 'B')",
		"INSERT INTO services VALUES ('sA', 'svcA', 'A'), ('sB', 'svcB', 'B')",
		"INSERT INTO diskinfo VALUES (1, 'lun1', ''), (2, 'lun2', '')",
		"INSERT INTO svcdisks VALUES (1, 'lun1', 'nA', 'sA'), (2, 'lun1', 'nB', 'sB'), (3, 'lun2', 'nB', 'sB')",
	)
	odb := testDashboardDB(db)
	ctx := context.Background()
	rows := func(groups []string, isManager bool) string {
		t.Helper()
		query, args, err := buildDisksQuery(groups, isManager, []string{"diskinfo.disk_id", "nodes.nodename"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		r, err := odb.DB.QueryContext(ctx, query+" ORDER BY svcdisks.id", args...)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = r.Close() }()
		var out []string
		for r.Next() {
			var disk, node string
			if err := r.Scan(&disk, &node); err != nil {
				t.Fatal(err)
			}
			out = append(out, disk+"@"+node)
		}
		return fmt.Sprint(out)
	}
	if got := rows([]string{"teamA"}, false); got != "[lun1@nodeA]" {
		t.Errorf("team A: %s", got)
	}
	if got := rows(nil, true); got != "[lun1@nodeA lun1@nodeB lun2@nodeB]" {
		t.Errorf("manager: %s", got)
	}
}
