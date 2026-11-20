package cdb

import (
	"context"
	"slices"
	"testing"
)

// TestAlertUnknownEnv checks an alert of a node whose environment is not one
// of the dashboard enum is written with an empty environment, and does not fail
// the alerts of the other nodes: the agent sets the environment freely.
func TestAlertUnknownEnv(t *testing.T) {
	db := testMariaDB(t, dashboardTestTables...)
	testExec(t, db,
		"INSERT INTO nodes (node_id, node_env, node_frozen) VALUES ('n1', 'UAT', 'T'), ('n2', 'PRD', 'T')",
	)
	if err := testDashboardDB(db).DashboardUpdateFrozen(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []testAlert{
		{Type: "node frozen", NodeID: "n1", Env: "", Severity: 1},
		{Type: "node frozen", NodeID: "n2", Env: "PRD", Severity: 1},
	}
	if got := testAlerts(t, db); !slices.Equal(got, want) {
		t.Fatalf("got %+v\nwant %+v", got, want)
	}
}
