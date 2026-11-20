package cdb

import (
	"context"
	"slices"
	"testing"
)

// TestDashboardNodeCloseToMaintenanceEnd checks the alert is raised on the
// nodes whose maintenance ends within 30 days only: not on a maintenance
// ending in a year, nor on one ended already.
func TestDashboardNodeCloseToMaintenanceEnd(t *testing.T) {
	db := testMariaDB(t,
		dashboardTestTables[0],
		"CREATE TABLE nodes (node_id char(36) CHARACTER SET ascii PRIMARY KEY, node_env varchar(256), maintenance_end datetime)",
		"INSERT INTO nodes VALUES ('soon', 'PRD', NOW() + INTERVAL 10 DAY), ('later', 'PRD', NOW() + INTERVAL 1 YEAR),"+
			" ('ended', 'PRD', NOW() - INTERVAL 10 DAY), ('none', 'PRD', NULL)",
	)
	if err := testDashboardDB(db).DashboardUpdateNodeCloseToMaintenanceEnd(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []testAlert{{Type: "node close to maintenance end", NodeID: "soon", Env: "PRD"}}
	if got := testAlerts(t, db); !slices.Equal(got, want) {
		t.Fatalf("got %+v\nwant %+v", got, want)
	}
}
