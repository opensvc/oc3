package cdb

import (
	"context"
	"slices"
	"testing"
)

// TestDashboardUpdateCompAttachDiff checks the alerts of the services whose
// nodes have not the same rulesets, or modulesets, attached: run on the
// tables of the collector, as the scheduler task alerts_1d does.
func TestDashboardUpdateCompAttachDiff(t *testing.T) {
	db := testMariaDB(t, append(slices.Clone(dashboardTestTables),
		"ALTER TABLE services ADD COLUMN updated datetime",
		"ALTER TABLE svcmon ADD COLUMN mon_svctype varchar(10)",
		"CREATE TABLE comp_rulesets (id int AUTO_INCREMENT PRIMARY KEY, ruleset_name varchar(128) NOT NULL)",
		"CREATE TABLE comp_rulesets_nodes (id int AUTO_INCREMENT PRIMARY KEY, ruleset_id int NOT NULL, node_id char(36) CHARACTER SET ascii)",
		"CREATE TABLE comp_moduleset (id int AUTO_INCREMENT PRIMARY KEY, modset_name varchar(128) NOT NULL)",
		"CREATE TABLE comp_node_moduleset (id int AUTO_INCREMENT PRIMARY KEY, modset_id int NOT NULL, node_id char(36) CHARACTER SET ascii)",
	)...)
	testExec(t, db,
		"INSERT INTO nodes (node_id, nodename) VALUES ('n1', 'node1'), ('n2', 'node2'), ('n3', 'node3')",
		// s1 on n1, n2 (PRD); s2 on n1, n3 (DEV); s3 on n1 only; s4 not updated lately
		"INSERT INTO services (svc_id, svc_env, updated) VALUES ('s1', 'PRD', NOW()), ('s2', 'DEV', NOW()), ('s3', 'DEV', NOW()),"+
			" ('s4', 'DEV', NOW() - INTERVAL 3 DAY)",
		"INSERT INTO svcmon (svc_id, node_id, mon_svctype) VALUES ('s1', 'n1', 'PRD'), ('s1', 'n2', 'PRD'),"+
			" ('s2', 'n1', 'DEV'), ('s2', 'n3', 'DEV'), ('s3', 'n1', 'DEV'), ('s4', 'n1', 'DEV'), ('s4', 'n2', 'DEV')",
		// r1 on n1, n2, n3; r2 on n1 only: s1 and s4 differ (n1/n2), s2 differs (n1/n3)
		"INSERT INTO comp_rulesets (id, ruleset_name) VALUES (1, 'r1'), (2, 'r2')",
		"INSERT INTO comp_rulesets_nodes (ruleset_id, node_id) VALUES (1, 'n1'), (1, 'n2'), (1, 'n3'), (2, 'n1')",
		// m1 on n1, n3; m2 on n1, n3: s1 differs twice (n1/n2), s2 does not
		"INSERT INTO comp_moduleset (id, modset_name) VALUES (1, 'm1'), (2, 'm2')",
		"INSERT INTO comp_node_moduleset (modset_id, node_id) VALUES (1, 'n1'), (1, 'n3'), (2, 'n1'), (2, 'n3')",
	)
	odb := testDashboardDB(db)
	ctx := context.Background()
	if err := odb.DashboardUpdateCompRsetDiff(ctx); err != nil {
		t.Fatal(err)
	}
	if err := odb.DashboardUpdateCompModDiff(ctx); err != nil {
		t.Fatal(err)
	}
	want := []testAlert{
		{Type: "compliance moduleset attachment differences in cluster", SvcID: "s1", Dict: `{"n":2,"nodes":"node1,node2"}`, Env: "PRD", Severity: 1},
		{Type: "compliance ruleset attachment differences in cluster", SvcID: "s1", Dict: `{"n":1,"nodes":"node1,node2"}`, Env: "PRD", Severity: 1},
		{Type: "compliance ruleset attachment differences in cluster", SvcID: "s2", Dict: `{"n":1,"nodes":"node1,node3"}`, Env: "DEV"},
	}
	if got := testAlerts(t, db); !slices.Equal(got, want) {
		t.Fatalf("got %+v\nwant %+v", got, want)
	}

	// r2 attached to n2 and n3 too: no more ruleset difference, the alerts
	// written before the run are purged.
	testExec(t, db, "INSERT INTO comp_rulesets_nodes (ruleset_id, node_id) VALUES (2, 'n2'), (2, 'n3')",
		"UPDATE dashboard SET dash_updated = NOW() - INTERVAL 1 MINUTE")
	if err := odb.DashboardUpdateCompRsetDiff(ctx); err != nil {
		t.Fatal(err)
	}
	if got := testAlerts(t, db); len(got) != 1 || got[0].Type != "compliance moduleset attachment differences in cluster" {
		t.Fatalf("got %+v, want the moduleset alert only", got)
	}
}

// TestCompAttachDiffNodesString checks the nodenames of the alert are cut
// below 50 characters, the last ones replaced by their count.
func TestCompAttachDiffNodesString(t *testing.T) {
	for _, tc := range []struct {
		nodes []string
		want  string
	}{
		{[]string{"a", "b"}, "a,b"},
		{[]string{"node-0123456789", "node-0123456789", "node-0123456789", "node-0123456789"}, "node-0123456789,node-0123456789, ... (+2)"},
		{[]string{"node-with-a-name-longer-than-fifty-characters-really"}, ", ... (+1)"},
	} {
		if got := compAttachDiffNodesString(tc.nodes); got != tc.want {
			t.Errorf("%q: got %q, want %q", tc.nodes, got, tc.want)
		}
	}
}
