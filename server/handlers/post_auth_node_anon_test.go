package serverhandlers

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/spf13/viper"

	"github.com/opensvc/oc3/cdb"
)

// TestPostAuthNodeAnonymous checks an anonymous registration: a new node gets
// its password, a node registered already does not, nor does its cluster
// change; the node itself registering again does get it back.
func TestPostAuthNodeAnonymous(t *testing.T) {
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
	name := fmt.Sprintf("oc3_test_anonreg_%d", time.Now().UnixNano())
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
	for _, s := range []string{
		"CREATE TABLE apps (id int AUTO_INCREMENT PRIMARY KEY, app varchar(64))",
		"CREATE TABLE nodes (id int AUTO_INCREMENT PRIMARY KEY, nodename varchar(128), team_responsible varchar(64), app varchar(64)," +
			" node_id char(36), cluster_id char(36) DEFAULT '', updated datetime)",
		"CREATE TABLE auth_node (id int AUTO_INCREMENT PRIMARY KEY, nodename varchar(128), uuid char(36), node_id char(36), updated datetime)",
		"CREATE TABLE log (id INT AUTO_INCREMENT PRIMARY KEY, log_action VARCHAR(100) NOT NULL, log_user VARCHAR(100) NOT NULL," +
			" log_impersonator VARCHAR(100), log_origin VARCHAR(16), log_request_id VARCHAR(64), log_fmt VARCHAR(200), log_dict TEXT," +
			" log_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, svc_id CHAR(36) DEFAULT '', log_level ENUM('debug','info','warning','error','critical') DEFAULT 'info'," +
			" node_id CHAR(36) DEFAULT '')",
		"CREATE TABLE cluster_declarations (cluster_id char(36) PRIMARY KEY)",
		"CREATE TABLE services (svc_id char(36) PRIMARY KEY, cluster_id char(36))",
		"CREATE TABLE clusters (cluster_id char(36) PRIMARY KEY)",
		"CREATE TABLE node_cluster_claims (node_id char(36) PRIMARY KEY, cluster_id char(36) NOT NULL, created datetime NOT NULL, updated datetime NOT NULL)",
		"INSERT INTO apps (app) VALUES ('app1')",
		// c2 has a member: a node claiming it waits for the member to confirm.
		"INSERT INTO nodes (nodename, team_responsible, app, node_id, cluster_id) VALUES ('m2', 'Everybody', 'app1', 'm2', 'c2')",
		"INSERT INTO nodes (nodename, team_responsible, app, node_id, cluster_id) VALUES ('node1', 'Everybody', 'app1', 'n1', 'c1')",
		"INSERT INTO auth_node (nodename, uuid, node_id) VALUES ('node1', 'secret-uuid', 'n1')",
	} {
		if _, err := pool.Exec(s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	initMetrics.Do(cdb.InitMetrics)
	odb := cdb.New(pool)
	odb.CreateSession(noEvents{})
	a := &Api{ODB: odb, SyncTimeout: 10 * time.Second}
	viper.Set("server.allow_anon_register", true)
	t.Cleanup(func() { viper.Set("server.allow_anon_register", false) })

	register := func(mode, nodename string, clusterID ...string) (int, map[string]any) {
		t.Helper()
		cluster := "c2"
		if len(clusterID) > 0 {
			cluster = clusterID[0]
		}
		c, rec := aiContext(http.MethodPost, "/auth/node",
			fmt.Sprintf(`{"nodename": %q, "app": "app1", "cluster_id": %q}`, nodename, cluster))
		c.Set(XAuthMode, mode)
		if mode == AuthModeNode {
			c.Set(XNodeID, "n1")
			c.Set(XNodename, nodename)
		}
		_ = a.PostAuthNode(c)
		var body map[string]any
		_ = json.Unmarshal(rec.Body.Bytes(), &body)
		return rec.Code, body
	}

	if code, body := register(AuthModeAnonRegister, "node1"); code != http.StatusForbidden || body["uuid"] != nil {
		t.Errorf("anonymous, registered node: %d %v", code, body)
	}
	var clusterID string
	if err := pool.QueryRow("SELECT cluster_id FROM nodes WHERE node_id = 'n1'").Scan(&clusterID); err != nil || clusterID != "c1" {
		t.Errorf("cluster of the registered node changed: %q, %v", clusterID, err)
	}
	if code, body := register(AuthModeNode, "node1"); code != http.StatusOK || body["uuid"] != "secret-uuid" {
		t.Errorf("the node itself: %d %v", code, body)
	}
	if code, body := register(AuthModeAnonRegister, "node2"); code != http.StatusOK || body["uuid"] == nil || body["uuid"] == "" {
		t.Errorf("anonymous, new node: %d %v", code, body)
	}
	cluster := func(nodename string) (clusterID, claimed string) {
		t.Helper()
		if err := pool.QueryRow("SELECT n.cluster_id, IFNULL(c.cluster_id, '') FROM nodes n LEFT JOIN node_cluster_claims c ON c.node_id = n.node_id"+
			" WHERE n.nodename = ?", nodename).Scan(&clusterID, &claimed); err != nil {
			t.Fatal(err)
		}
		return
	}
	if got, claim := cluster("node1"); got != "c1" || claim != "c2" {
		t.Errorf("the node itself claiming a cluster with members: cluster %q claim %q, want c1 c2", got, claim)
	}
	if got, claim := cluster("node2"); got != "" || claim != "c2" {
		t.Errorf("new node claiming a cluster with members: cluster %q claim %q, want '' c2", got, claim)
	}
	if code, _ := register(AuthModeNode, "node1", "c3"); code != http.StatusOK {
		t.Errorf("the node itself, new cluster: %d", code)
	}
	if got, claim := cluster("node1"); got != "c3" || claim != "" {
		t.Errorf("the node itself claiming a new cluster: cluster %q claim %q, want c3 ''", got, claim)
	}
}
