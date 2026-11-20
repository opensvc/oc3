package cdb

import (
	"context"
	"database/sql"
	"slices"
	"testing"
	"time"
)

const (
	testClaimClusterA = "aaaaaaaa-0000-0000-0000-000000000000"
	testClaimClusterB = "bbbbbbbb-0000-0000-0000-000000000000"
	testClaimClusterD = "dddddddd-0000-0000-0000-000000000000"
	testClaimClusterO = "00000000-0000-0000-0000-00000000000f"
	testClaimClusterN = "eeeeeeee-0000-0000-0000-000000000000"
)

func testClaimDB(t *testing.T) *sql.DB {
	t.Helper()
	return testMariaDB(t,
		dashboardTestTables[0],
		"CREATE TABLE nodes (node_id char(36) CHARACTER SET ascii PRIMARY KEY, nodename varchar(60), node_env varchar(256), cluster_id char(36) CHARACTER SET ascii DEFAULT '')",
		"CREATE TABLE cluster_declarations (cluster_id char(36) CHARACTER SET ascii PRIMARY KEY)",
		"CREATE TABLE services (svc_id char(36) CHARACTER SET ascii PRIMARY KEY, cluster_id char(36) CHARACTER SET ascii)",
		"CREATE TABLE clusters (cluster_id char(36) CHARACTER SET ascii PRIMARY KEY)",
		"CREATE TABLE node_cluster_claims (node_id char(36) CHARACTER SET ascii PRIMARY KEY, cluster_id char(36) CHARACTER SET ascii NOT NULL, created datetime NOT NULL, updated datetime NOT NULL)",
		// cluster A: a1, a2; cluster D declared, no node; cluster O: a service, no node
		"INSERT INTO nodes VALUES ('a1', 'a1', 'PRD', '"+testClaimClusterA+"'), ('a2', 'a2', 'PRD', '"+testClaimClusterA+"'),"+
			" ('x', 'x', 'PRD', '"+testClaimClusterB+"'), ('j', 'j', 'PRD', ''), ('k', 'a2', 'DEV', '')",
		"INSERT INTO cluster_declarations VALUES ('"+testClaimClusterD+"')",
		"INSERT INTO services VALUES ('s1', '"+testClaimClusterO+"')",
	)
}

func testNodeCluster(t *testing.T, db *sql.DB, nodeID string) (clusterID, claimed string) {
	t.Helper()
	if err := db.QueryRow("SELECT cluster_id FROM nodes WHERE node_id = ?", nodeID).Scan(&clusterID); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT cluster_id FROM node_cluster_claims WHERE node_id = ?", nodeID).Scan(&claimed); err != nil && err != sql.ErrNoRows {
		t.Fatal(err)
	}
	return
}

// TestClaimClusterMembership checks a node becomes a member of a cluster the
// collector does not know, or knows without node, at once, and only claims a
// cluster having members or declared.
func TestClaimClusterMembership(t *testing.T) {
	db := testClaimDB(t)
	odb := testDashboardDB(db)
	ctx := context.Background()
	for _, tc := range []struct {
		name, node, cluster string
		want                ClusterClaim
		wantCluster         string
		wantClaim           string
	}{
		{"member", "a1", testClaimClusterA, ClusterClaimMember, testClaimClusterA, ""},
		{"known cluster, pending", "x", testClaimClusterA, ClusterClaimPending, testClaimClusterB, testClaimClusterA},
		{"declared cluster, pending", "j", testClaimClusterD, ClusterClaimPending, "", testClaimClusterD},
		{"orphan cluster, taken over", "j", testClaimClusterO, ClusterClaimOrphan, testClaimClusterO, ""},
		{"new cluster", "x", testClaimClusterN, ClusterClaimNew, testClaimClusterN, ""},
		{"empty", "a1", "", ClusterClaimNone, testClaimClusterA, ""},
		{"unknown node", "nope", testClaimClusterA, ClusterClaimNone, "", ""},
	} {
		got, err := odb.ClaimClusterMembership(ctx, tc.node, tc.cluster)
		if err != nil {
			t.Fatalf("%s: %s", tc.name, err)
		}
		if got != tc.want {
			t.Errorf("%s: got %s, want %s", tc.name, got, tc.want)
		}
		if tc.node == "nope" {
			continue
		}
		if cluster, claim := testNodeCluster(t, db, tc.node); cluster != tc.wantCluster || claim != tc.wantClaim {
			t.Errorf("%s: cluster %q claim %q, want %q %q", tc.name, cluster, claim, tc.wantCluster, tc.wantClaim)
		}
	}
}

// TestConfirmClusterClaims checks a member listing a node claiming its
// cluster confirms it, but not a name claimed by several nodes, or borne by
// a member already, nor a node claiming another cluster.
func TestConfirmClusterClaims(t *testing.T) {
	db := testClaimDB(t)
	odb := testDashboardDB(db)
	ctx := context.Background()
	testExec(t, db,
		"INSERT INTO nodes VALUES ('n3', 'a3', 'PRD', ''), ('r1', 'r', 'PRD', ''), ('r2', 'r', 'PRD', ''), ('o', 'o', 'PRD', '')",
		"INSERT INTO node_cluster_claims VALUES"+
			" ('n3', '"+testClaimClusterA+"', NOW(), NOW()),"+ // joined
			" ('k', '"+testClaimClusterA+"', NOW(), NOW()),"+ // a2 is a member already
			" ('r1', '"+testClaimClusterA+"', NOW(), NOW()), ('r2', '"+testClaimClusterA+"', NOW(), NOW()),"+ // two r
			" ('o', '"+testClaimClusterB+"', NOW(), NOW())", // another cluster
	)
	got, err := odb.ConfirmClusterClaims(ctx, testClaimClusterA, []string{"a1", "a2", "A3", "r", "o"})
	if err != nil {
		t.Fatal(err)
	}
	if want := []ConfirmedClusterClaim{{NodeID: "n3", Nodename: "a3"}}; !slices.Equal(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
	for node, want := range map[string][2]string{
		"n3": {testClaimClusterA, ""},
		"k":  {"", testClaimClusterA},
		"r1": {"", testClaimClusterA},
		"r2": {"", testClaimClusterA},
		"o":  {"", testClaimClusterB},
	} {
		if cluster, claim := testNodeCluster(t, db, node); cluster != want[0] || claim != want[1] {
			t.Errorf("%s: cluster %q claim %q, want %q %q", node, cluster, claim, want[0], want[1])
		}
	}
}

// TestDashboardNodeClusterClaims checks the alert is raised on a claim older
// than the delay only, and removed with the claim.
func TestDashboardNodeClusterClaims(t *testing.T) {
	db := testClaimDB(t)
	odb := testDashboardDB(db)
	ctx := context.Background()
	testExec(t, db,
		"INSERT INTO node_cluster_claims VALUES ('x', '"+testClaimClusterA+"', NOW() - INTERVAL 1 HOUR, NOW()),"+
			" ('j', '"+testClaimClusterA+"', NOW(), NOW())",
	)
	if err := odb.DashboardUpdateNodeClusterClaims(ctx, 5*time.Minute); err != nil {
		t.Fatal(err)
	}
	want := []testAlert{{Type: "node cluster not confirmed", NodeID: "x", Env: "PRD", Severity: 2,
		Dict: `{"cluster_id": "` + testClaimClusterA + `"}`}}
	if got := testAlerts(t, db); !slices.Equal(got, want) {
		t.Fatalf("got %+v\nwant %+v", got, want)
	}
	testExec(t, db, "DELETE FROM node_cluster_claims WHERE node_id = 'x'", "UPDATE dashboard SET dash_updated = NOW() - INTERVAL 1 MINUTE")
	if err := odb.DashboardUpdateNodeClusterClaims(ctx, 5*time.Minute); err != nil {
		t.Fatal(err)
	}
	if got := testAlerts(t, db); len(got) != 0 {
		t.Fatalf("got %+v, want none", got)
	}
}
