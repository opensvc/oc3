package serverhandlers

import (
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/opensvc/oc3/cdb"
)

// TestDeleteClusterRefused checks who may delete a cluster: a user holding
// NodeManager, responsible for every node and every service of the cluster.
// The cascade itself is tested in cdb.
func TestDeleteClusterRefused(t *testing.T) {
	dsn := os.Getenv("OC3_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("OC3_TEST_MYSQL_DSN is not set, as root:pass@tcp(127.0.0.1:3306)/: no MariaDB to test against")
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.InterpolateParams = true
	cfg.DBName = ""
	admin, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	name := fmt.Sprintf("oc3_test_delcluster_%d", time.Now().UnixNano())
	if _, err := admin.Exec("CREATE DATABASE " + name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec("DROP DATABASE " + name) })
	cfg.DBName = name
	pool, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pool.Close() })
	const clusterID = "c0000000-0000-0000-0000-000000000001"
	for _, s := range []string{
		"CREATE TABLE clusters (cluster_id char(36), cluster_name varchar(128))",
		"CREATE TABLE nodes (node_id char(36), nodename varchar(128), app varchar(64), cluster_id char(36))",
		"CREATE TABLE services (svc_id char(36), svcname varchar(128), svc_app varchar(64), cluster_id char(36))",
		"CREATE TABLE apps (id int, app varchar(64))",
		"CREATE TABLE apps_responsibles (app_id int, group_id int)",
		"CREATE TABLE auth_group (id int, role varchar(64))",
		"INSERT INTO clusters VALUES ('" + clusterID + "', 'prdclu')",
		"INSERT INTO nodes VALUES ('n1', 'node1', 'app1', '" + clusterID + "'), ('n2', 'node2', 'app1', '" + clusterID + "')",
		"INSERT INTO services VALUES ('s1', 'svc1', 'app1', '" + clusterID + "'), ('s2', 'svc2', 'app2', '" + clusterID + "')",
		"INSERT INTO apps VALUES (1, 'app1'), (2, 'app2')",
		"INSERT INTO apps_responsibles VALUES (1, 10), (1, 11), (2, 11)",
		"INSERT INTO auth_group VALUES (10, 'ops1'), (11, 'ops12')",
	} {
		if _, err := pool.Exec(s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	initMetrics.Do(cdb.InitMetrics)
	odb := cdb.New(pool)
	odb.CreateSession(noEvents{})
	a := &Api{ODB: odb, SyncTimeout: 10 * time.Second}

	for _, tc := range []struct {
		what      string
		clusterID string
		groups    []string
		code      int
	}{
		{"without NodeManager", clusterID, []string{"ops12"}, http.StatusForbidden},
		{"unknown cluster", "c0000000-0000-0000-0000-000000000009", []string{"NodeManager", "ops12"}, http.StatusNotFound},
		{"not responsible for a service", clusterID, []string{"NodeManager", "ops1"}, http.StatusForbidden},
		{"not responsible for the nodes", clusterID, []string{"NodeManager", "other"}, http.StatusForbidden},
	} {
		c, rec := aiContext(http.MethodDelete, "/clusters/"+tc.clusterID, "", tc.groups...)
		_ = a.DeleteCluster(c, tc.clusterID)
		if rec.Code != tc.code {
			t.Errorf("%s: %d %s, want %d", tc.what, rec.Code, rec.Body.String(), tc.code)
		}
	}
	c, rec := aiContext(http.MethodDelete, "/clusters/"+clusterID, "")
	c.Set(XAuthMode, AuthModeNode)
	_ = a.DeleteCluster(c, clusterID)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("as a node: %d, want 401", rec.Code)
	}
	var n int
	if err := pool.QueryRow("SELECT COUNT(*) FROM nodes").Scan(&n); err != nil || n != 2 {
		t.Errorf("nodes left %d, %v: nothing deleted", n, err)
	}
}
