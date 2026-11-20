package cdb

import (
	"context"
	"fmt"
	"testing"
)

// TestGetLogsVisibility checks the log events a user reads: their own, those
// of the nodes and services of their apps; a Manager reads them all.
func TestGetLogsVisibility(t *testing.T) {
	db := testMariaDB(t,
		"CREATE TABLE log (id int AUTO_INCREMENT PRIMARY KEY, log_user varchar(100), node_id char(36) DEFAULT '', svc_id char(36) DEFAULT '')",
		"CREATE TABLE nodes (node_id char(36), app varchar(64))",
		"CREATE TABLE services (svc_id char(36), svc_app varchar(64))",
		"CREATE TABLE apps (id int, app varchar(64))",
		"CREATE TABLE apps_responsibles (app_id int, group_id int)",
		"CREATE TABLE auth_group (id int, role varchar(64))",
		"INSERT INTO nodes VALUES ('nA', 'A'), ('nB', 'B')",
		"INSERT INTO services VALUES ('sA', 'A'), ('sB', 'B')",
		"INSERT INTO apps VALUES (1, 'A'), (2, 'B')",
		"INSERT INTO apps_responsibles VALUES (1, 10), (2, 20)",
		"INSERT INTO auth_group VALUES (10, 'teamA'), (20, 'teamB')",
		// 1 own, 2 node A, 3 service A, 4 node B, 5 service B, 6 another user's change of a group
		"INSERT INTO log (log_user, node_id, svc_id) VALUES ('me@x', '', ''), ('other@x', 'nA', ''), ('other@x', '', 'sA'),"+
			" ('other@x', 'nB', ''), ('other@x', '', 'sB'), ('other@x', '', '')",
	)
	odb := testDashboardDB(db)
	ctx := context.Background()
	ids := func(p ListParams, user string) string {
		t.Helper()
		p.SelectExprs, p.Props = []string{"log.id"}, []string{"id"}
		p.OrderBy = nil
		rows, err := odb.GetLogs(ctx, p, LogsFiltersetFilter{}, user)
		if err != nil {
			t.Fatal(err)
		}
		var out []any
		for i := len(rows) - 1; i >= 0; i-- {
			out = append(out, rows[i]["id"])
		}
		return fmt.Sprint(out)
	}
	if got := ids(ListParams{Groups: []string{"teamA"}}, "me@x"); got != "[1 2 3]" {
		t.Errorf("user of app A: %s", got)
	}
	if got := ids(ListParams{}, "me@x"); got != "[1]" {
		t.Errorf("user of no app: %s", got)
	}
	if got := ids(ListParams{IsManager: true}, "me@x"); got != "[1 2 3 4 5 6]" {
		t.Errorf("manager: %s", got)
	}
	p := ListParams{Groups: []string{"teamA"}, SelectExprs: []string{"log.id"}, Props: []string{"id"}}
	if rows, err := odb.GetLog(ctx, "4", p, "me@x"); err != nil || len(rows) != 0 {
		t.Errorf("event of node B: %v, %v", rows, err)
	}
	if rows, err := odb.GetLog(ctx, "2", p, "me@x"); err != nil || len(rows) != 1 {
		t.Errorf("event of node A: %v, %v", rows, err)
	}
}
