package cdb

import (
	"context"
	"database/sql"
	"testing"
)

var nodeUpTestTables = []string{
	"CREATE TABLE nodes (node_id char(36) CHARACTER SET ascii PRIMARY KEY, nodename varchar(256)," +
		" cluster_id char(36) CHARACTER SET ascii DEFAULT '', last_comm datetime(3), version varchar(64) DEFAULT 'v3.0.0'," +
		" os_name varchar(64) DEFAULT 'Linux', action_type varchar(8) DEFAULT NULL, collector varchar(128) DEFAULT NULL," +
		" connect_to varchar(128) DEFAULT NULL)",
	"CREATE TABLE hbmon (id bigint AUTO_INCREMENT PRIMARY KEY, cluster_id char(36) CHARACTER SET ascii DEFAULT ''," +
		" node_id char(36) CHARACTER SET ascii DEFAULT '', peer_node_id char(36) CHARACTER SET ascii DEFAULT ''," +
		" name varchar(64) DEFAULT '', beating tinyint(1) DEFAULT 0)",
	"CREATE TABLE action_queue (id int AUTO_INCREMENT PRIMARY KEY, status varchar(1) DEFAULT 'W', command text NOT NULL," +
		" date_dequeued timestamp NOT NULL DEFAULT '0000-00-00 00:00:00', stdout text DEFAULT NULL," +
		" node_id char(36) CHARACTER SET ascii DEFAULT '')",
}

// The cluster c1 pings, w2 alone in its last status: w1 and w3 are down,
// though the pings keep their last_comm as fresh as the one of w2. The
// cluster c2 has no heartbeat row: its node is up by its last_comm. The
// cluster c3 is silent for 10 minutes: its node is down, its heartbeat rows
// left by its last status.
func nodeUpTestDB(t *testing.T) (*DB, *sql.DB) {
	t.Helper()
	db := testMariaDB(t, nodeUpTestTables...)
	testExec(t, db,
		"INSERT INTO nodes (node_id, nodename, cluster_id, last_comm) VALUES"+
			" ('w1', 'w1', 'c1', NOW()), ('w2', 'w2', 'c1', NOW()), ('w3', 'w3', 'c1', NOW()),"+
			" ('s1', 's1', 'c2', NOW()),"+
			" ('o1', 'o1', 'c3', NOW() - INTERVAL 10 MINUTE)",
		"INSERT INTO hbmon (cluster_id, node_id, peer_node_id, name) VALUES"+
			" ('c1', 'w2', '', '1.rx'), ('c1', 'w2', '', '1.tx'),"+
			" ('c3', 'o1', '', '1.rx')",
	)
	return testDashboardDB(db), db
}

func TestNodeUp(t *testing.T) {
	odb, _ := nodeUpTestDB(t)
	for nodeID, want := range map[string]bool{
		"w1": false, "w2": true, "w3": false, "s1": true, "o1": false, "unknown": false,
	} {
		got, err := odb.NodeUp(context.Background(), nodeID)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("%s: got up %v, want %v", nodeID, got, want)
		}
	}
}

// The peer a node is woken from is up: w2, never w1 first by its name, and
// none when the node to wake is the only one up.
func TestClusterUpNodeExcept(t *testing.T) {
	odb, _ := nodeUpTestDB(t)
	for except, want := range map[string]string{"w3": "w2", "w1": "w2", "w2": ""} {
		peer, err := odb.ClusterUpNodeExcept(context.Background(), "c1", except)
		if err != nil {
			t.Fatal(err)
		}
		got := ""
		if peer != nil {
			got = peer.Nodename
		}
		if got != want {
			t.Errorf("except %s: got peer %q, want %q", except, got, want)
		}
	}
}

func TestActionQSetCancelled(t *testing.T) {
	odb, db := nodeUpTestDB(t)
	testExec(t, db, "INSERT INTO action_queue (id, status, command, node_id) VALUES (1, 'S', 'wake --node w2', 'w1')")
	if err := odb.ActionQSetCancelled(context.Background(), 1, "node w2 is up"); err != nil {
		t.Fatal(err)
	}
	var status, stdout string
	var dequeued bool
	if err := db.QueryRow("SELECT status, stdout, date_dequeued > 0 FROM action_queue WHERE id = 1").Scan(&status, &stdout, &dequeued); err != nil {
		t.Fatal(err)
	}
	if status != "C" || stdout != "node w2 is up" || !dequeued {
		t.Errorf("got status %q stdout %q dequeued %v", status, stdout, dequeued)
	}
}
