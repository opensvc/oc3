package serverhandlers

import (
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

// TestLogsRefuseNodes checks a node reads no log event, before any lookup:
// the Api has no database.
func TestLogsRefuseNodes(t *testing.T) {
	a := &Api{}
	c, rec := aiContext(http.MethodGet, "/logs", "")
	nodeAuth(c)
	if err := a.GetLogs(c, server.GetLogsParams{}); err == nil || rec.Code != http.StatusUnauthorized {
		t.Errorf("GET /logs as a node: %d, %v", rec.Code, err)
	}
	c, rec = aiContext(http.MethodGet, "/logs/1", "")
	nodeAuth(c)
	if err := a.GetLog(c, "1", server.GetLogParams{}); err == nil || rec.Code != http.StatusUnauthorized {
		t.Errorf("GET /logs/1 as a node: %d, %v", rec.Code, err)
	}
}

// TestRequireNodeVisible checks who sees a node, as GET /nodes/{node_id}/stats
// asks: a user responsible for its app, a Manager, the node itself; the others
// are answered 404.
func TestRequireNodeVisible(t *testing.T) {
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
	name := fmt.Sprintf("oc3_test_nodevis_%d", time.Now().UnixNano())
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
		"CREATE TABLE nodes (node_id char(36), app varchar(64))",
		"CREATE TABLE apps (id int, app varchar(64))",
		"CREATE TABLE apps_responsibles (app_id int, group_id int)",
		"CREATE TABLE auth_group (id int, role varchar(64))",
		"INSERT INTO nodes VALUES ('n1', 'A')",
		"INSERT INTO apps VALUES (1, 'A')",
		"INSERT INTO apps_responsibles VALUES (1, 10)",
		"INSERT INTO auth_group VALUES (10, 'teamA')",
	} {
		if _, err := pool.Exec(s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	initMetrics.Do(cdb.InitMetrics)
	odb := cdb.New(pool)
	odb.CreateSession(noEvents{})
	a := &Api{ODB: odb, SyncTimeout: 10 * time.Second}
	node := &cdb.DBNode{NodeID: "n1", Nodename: "node1"}
	for _, tc := range []struct {
		what   string
		groups []string
		node   string
		code   int
	}{
		{"responsible user", []string{"teamA"}, "", 0},
		{"manager", []string{"Manager"}, "", 0},
		{"other user", []string{"teamB"}, "", http.StatusNotFound},
		{"the node itself", nil, "n1", 0},
		{"another node", nil, "n2", http.StatusNotFound},
	} {
		c, rec := aiContext(http.MethodGet, "/nodes/n1/stats", "", tc.groups...)
		if tc.node != "" {
			nodeAuth(c)
			c.Set(XNodeID, tc.node)
		}
		err := a.requireNodeVisible(c, slog.Default(), node)
		if tc.code == 0 && err != nil || tc.code != 0 && (err == nil || rec.Code != tc.code) {
			t.Errorf("%s: %d, %v", tc.what, rec.Code, err)
		}
	}
}
