package cdb

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"testing"
)

// TestDeleteClusterCascade deletes a cluster: its nodes and services with the
// rows referring to them, and its own rows, every table; the other cluster
// and the history of the orchestration runs stay.
func TestDeleteClusterCascade(t *testing.T) {
	tables := slices.Concat(nodeCascadeTables, serviceCascadeTables, clusterCascadeTables, []string{"orchestration_run_targets"})
	slices.Sort(tables)
	tables = slices.Compact(tables)
	// Every table reduced to the columns the cascade reads.
	var statements []string
	for _, table := range tables {
		statements = append(statements, "CREATE TABLE "+table+" (node_id char(36) DEFAULT '', svc_id char(36) DEFAULT '',"+
			" cluster_id char(36) DEFAULT '')")
	}
	db := testMariaDB(t, statements...)
	const (
		gone = "c0000000-0000-0000-0000-000000000001"
		kept = "c0000000-0000-0000-0000-000000000002"
	)
	// In each table, a row of each node and service of each cluster, and one
	// naming each cluster alone.
	members := [][3]string{
		{"n1", "", ""}, {"n2", "", ""}, {"", "s1", ""}, {"", "", gone},
		{"n3", "", ""}, {"", "s3", ""}, {"", "", kept},
	}
	var inserts []string
	for _, table := range tables {
		for _, m := range members {
			inserts = append(inserts, fmt.Sprintf("INSERT INTO %s VALUES ('%s', '%s', '%s')", table, m[0], m[1], m[2]))
		}
	}
	// The nodes and services of the clusters, in the tables naming them.
	testExec(t, db, append(inserts,
		"UPDATE nodes SET cluster_id = '"+gone+"' WHERE node_id IN ('n1', 'n2')",
		"UPDATE nodes SET cluster_id = '"+kept+"' WHERE node_id = 'n3'",
		"UPDATE services SET cluster_id = '"+gone+"' WHERE svc_id = 's1'",
		"UPDATE services SET cluster_id = '"+kept+"' WHERE svc_id = 's3'",
	)...)

	odb := testDashboardDB(db)
	ctx := context.Background()
	var nodeIDs []string
	var services int
	err := odb.RunInTx(ctx, &sql.TxOptions{}, func(tx *DB) error {
		var err error
		nodeIDs, services, err = tx.DeleteClusterCascade(ctx, gone)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(nodeIDs) != "[n1 n2]" || services != 1 {
		t.Errorf("deleted nodes %v, %d services", nodeIDs, services)
	}
	// A row goes when its table is keyed by what it names of the cluster gone.
	for _, table := range tables {
		want := 0
		for _, m := range members {
			byNode := slices.Contains(nodeCascadeTables, table) && (m[0] == "n1" || m[0] == "n2")
			bySvc := slices.Contains(serviceCascadeTables, table) && m[1] == "s1"
			byCluster := slices.Contains(clusterCascadeTables, table) && m[2] == gone
			if !byNode && !bySvc && !byCluster {
				want++
			}
		}
		if table == "nodes" || table == "services" {
			// Their members name the cluster: n1, n2 and s1 went by their id.
			want = len(members) - map[string]int{"nodes": 2, "services": 1}[table]
			if slices.Contains(clusterCascadeTables, table) {
				t.Fatalf("%s is not keyed by cluster_id", table)
			}
		}
		var n int
		if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != want {
			t.Errorf("%s: %d rows left, want %d", table, n, want)
		}
	}
	for _, table := range []string{"nodes", "services", "orchestration_run_targets"} {
		var n int
		if err := db.QueryRow("SELECT COUNT(*) FROM " + table + " WHERE node_id = 'n3' OR svc_id = 's3' OR cluster_id = '" + kept + "'").Scan(&n); err != nil || n == 0 {
			t.Errorf("%s: the other cluster is gone (%d, %v)", table, n, err)
		}
	}
	if _, _, err := odb.DeleteClusterCascade(ctx, ""); err == nil {
		t.Error("an empty cluster_id is refused")
	}
}
