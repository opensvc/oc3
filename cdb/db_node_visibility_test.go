package cdb

import (
	"context"
	"testing"
)

// TestNodeSubListsVisibility checks the sub-lists of a node: its checks follow
// the responsibility for its app, as the nodes list; its alerts and its
// compliance follow the publication of its app, as the alerts and compliance
// lists. Neither follows its team_responsible: a node registered anonymously,
// of team Everybody, is not shown to every user.
func TestNodeSubListsVisibility(t *testing.T) {
	db := testMariaDB(t,
		"CREATE TABLE nodes (node_id char(36), nodename varchar(64), app varchar(64), team_responsible varchar(64))",
		"CREATE TABLE services (svc_id char(36), svcname varchar(64), svc_app varchar(64))",
		"CREATE TABLE apps (id int, app varchar(64))",
		"CREATE TABLE apps_responsibles (app_id int, group_id int)",
		"CREATE TABLE apps_publications (app_id int, group_id int)",
		"CREATE TABLE auth_group (id int, role varchar(64))",
		"CREATE TABLE checks_live (id int, node_id char(36), svc_id char(36), chk_type varchar(32), chk_instance varchar(64))",
		"CREATE TABLE dashboard (id int, node_id char(36), svc_id char(36))",
		"CREATE TABLE comp_log (id int, node_id char(36), svc_id char(36), run_date datetime)",
		"INSERT INTO apps VALUES (1, 'A')",
		// teamA is responsible for app A, which is published to teamA and teamP.
		"INSERT INTO apps_responsibles VALUES (1, 10)",
		"INSERT INTO apps_publications VALUES (1, 10), (1, 30)",
		"INSERT INTO auth_group VALUES (10, 'teamA'), (20, 'teamB'), (30, 'teamP')",
		"INSERT INTO nodes VALUES ('n1', 'node1', 'A', 'Everybody')",
		"INSERT INTO checks_live VALUES (1, 'n1', '', 'fs_u', '/')",
		"INSERT INTO dashboard VALUES (1, 'n1', '')",
		"INSERT INTO comp_log VALUES (1, 'n1', '', NOW())",
	)
	odb := testDashboardDB(db)
	ctx := context.Background()
	for _, tc := range []struct {
		groups            []string
		checks, published int
	}{{[]string{"teamA"}, 1, 1}, {[]string{"teamP"}, 0, 1}, {[]string{"teamB"}, 0, 0}, {nil, 0, 0}} {
		checks, err := odb.GetNodeChecks(ctx, "n1", ListParams{Groups: tc.groups, SelectExprs: []string{"checks_live.id"}, Props: []string{"id"}})
		if err != nil {
			t.Fatal(err)
		}
		alerts, err := odb.GetNodeAlerts(ctx, "n1", ListParams{Groups: tc.groups, SelectExprs: []string{"dashboard.id"}, Props: []string{"id"}})
		if err != nil {
			t.Fatal(err)
		}
		logs, err := odb.GetNodeComplianceLogs(ctx, "n1", ListParams{Groups: tc.groups, SelectExprs: []string{"comp_log.id"}, Props: []string{"id"}})
		if err != nil {
			t.Fatal(err)
		}
		ids, err := odb.PublishedNodeIDsForGroups(ctx, tc.groups)
		if err != nil {
			t.Fatal(err)
		}
		if len(checks) != tc.checks || len(alerts) != tc.published || len(logs) != tc.published || len(ids) != tc.published {
			t.Errorf("groups %v: %d checks (want %d), %d alerts, %d compliance logs, %d nodes (want %d)",
				tc.groups, len(checks), tc.checks, len(alerts), len(logs), len(ids), tc.published)
		}
	}
}
