package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// nodeUpCond is the SQL condition of a node n up now, as the last status of
// its cluster tells:
//   - the cluster still reports: its pings, sent every minute at least,
//     refresh the last_comm of every node of the cluster, down ones included,
//     so it says the cluster is alive, not the node,
//   - the node is in the last status: the speaker writes a heartbeat row of
//     its own for each node it has a heartbeat of, the rows of a node dropped
//     purged by the next status. A cluster with no heartbeat row at all, as a
//     node with no heartbeat configured, has only last_comm to tell.
const nodeUpCond = "n.last_comm > NOW() - INTERVAL 3 MINUTE" +
	" AND (EXISTS (SELECT 1 FROM hbmon h WHERE h.node_id = n.node_id AND h.peer_node_id = '')" +
	" OR NOT EXISTS (SELECT 1 FROM hbmon h WHERE h.cluster_id = n.cluster_id))"

// NodeUp tells whether the node is up now, as the last status of its cluster
// says: a node absent is not.
func (oDb *DB) NodeUp(ctx context.Context, nodeID string) (bool, error) {
	var one int
	err := oDb.DB.QueryRowContext(ctx, "SELECT 1 FROM nodes n WHERE n.node_id = ? AND "+nodeUpCond, nodeID).Scan(&one)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("nodeUp: %w", err)
	}
	return true, nil
}

// ClusterUpNodeExcept returns a node of the cluster up now, never the node
// exceptNodeID, running the om3 agent ("v3." or "3." versions) and reading its
// own queue (pull or feed), to run the wake of exceptNodeID: nil when there is
// none.
func (oDb *DB) ClusterUpNodeExcept(ctx context.Context, clusterID, exceptNodeID string) (*NodeActionTarget, error) {
	const query = "SELECT n.node_id, COALESCE(n.nodename, ''), COALESCE(n.os_name, ''), COALESCE(n.action_type, '')," +
		" COALESCE(n.collector, ''), COALESCE(n.connect_to, '')" +
		" FROM nodes n WHERE n.cluster_id = ? AND n.node_id <> ? AND " + nodeUpCond +
		" AND (n.version LIKE 'v3.%' OR n.version LIKE '3.%')" +
		" AND COALESCE(n.action_type, '') IN ('', 'pull', 'feed')" +
		" ORDER BY n.nodename LIMIT 1"
	var t NodeActionTarget
	err := oDb.DB.QueryRowContext(ctx, query, clusterID, exceptNodeID).
		Scan(&t.NodeID, &t.Nodename, &t.OSName, &t.ActionType, &t.Collector, &t.ConnectTo)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("clusterUpNodeExcept: %w", err)
	}
	return &t, nil
}

// ActionQSetCancelled closes the action not run, as a cancellation from the
// action queue view does, the reason in its stdout.
func (oDb *DB) ActionQSetCancelled(ctx context.Context, id int, reason string) error {
	const query = "UPDATE action_queue SET status = 'C', date_dequeued = NOW(), stdout = ? WHERE id = ?"
	if _, err := oDb.ExecContext(ctx, query, reason, id); err != nil {
		return fmt.Errorf("actionQSetCancelled: %w", err)
	}
	oDb.SetChange("action_queue")
	return nil
}
