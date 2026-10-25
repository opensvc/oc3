package cdb

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type DBAuthNode struct {
	ID       int64
	Nodename string
	UUID     string
	NodeID   string
	Updated  string
}

// return the list of auth_node rows matching the given node_id
func (oDb *DB) AuthNodesByNodeID(ctx context.Context, nodeID string) ([]DBAuthNode, error) {
	defer logDuration("AuthNodesByNodeID", time.Now())
	const query = `SELECT id, nodename, uuid, node_id, COALESCE(updated, '') FROM auth_node WHERE node_id = ?`
	rows, err := oDb.DB.QueryContext(ctx, query, nodeID)
	if err != nil {
		return nil, fmt.Errorf("AuthNodesByNodeID: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var result []DBAuthNode
	for rows.Next() {
		var r DBAuthNode
		var nodename, uid, nid sql.NullString
		if err := rows.Scan(&r.ID, &nodename, &uid, &nid, &r.Updated); err != nil {
			return nil, fmt.Errorf("AuthNodesByNodeID scan: %w", err)
		}
		r.Nodename = nodename.String
		r.UUID = uid.String
		r.NodeID = nid.String
		result = append(result, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("AuthNodesByNodeID rows: %w", err)
	}
	return result, nil
}

// UpdateAuthNodeNodename updates the nodename in auth_node for a given node_id so the node
// does not have to re-register after a hostname change.
func (oDb *DB) UpdateAuthNodeNodename(ctx context.Context, nodeID, nodename string) error {
	defer logDuration("UpdateAuthNodeNodename", time.Now())
	const query = `UPDATE auth_node SET nodename = ?, updated = NOW() WHERE node_id = ?`
	if _, err := oDb.DB.ExecContext(ctx, query, nodename, nodeID); err != nil {
		return fmt.Errorf("UpdateAuthNodeNodename: %w", err)
	}
	oDb.SetChange("auth_node")
	return nil
}

// InsertAuthNode inserts a new auth_node row with the given nodename, uuid, and node_id.
func (oDb *DB) InsertAuthNode(ctx context.Context, nodename, nodeUUID, nodeID string) error {
	defer logDuration("InsertAuthNode", time.Now())
	const query = `INSERT INTO auth_node (nodename, uuid, node_id) VALUES (?, ?, ?)`
	if _, err := oDb.ExecContext(ctx, query, nodename, nodeUUID, nodeID); err != nil {
		return fmt.Errorf("InsertAuthNode: %w", err)
	}
	oDb.SetChange("auth_node")
	return nil
}

// NodeIDsByNodenameAndAuthUUID returns the node_id of the nodes named
// nodename authenticated by nodeUUID. The uuid is a password, not unique
// among the auth_node rows, so it designates a node only with its nodename.
func (oDb *DB) NodeIDsByNodenameAndAuthUUID(ctx context.Context, nodename, nodeUUID string) ([]string, error) {
	defer logDuration("NodeIDsByNodenameAndAuthUUID", time.Now())
	const query = `SELECT a.node_id FROM auth_node a JOIN nodes n ON n.node_id = a.node_id
		WHERE a.nodename = ? AND a.uuid = ?`
	rows, err := oDb.DB.QueryContext(ctx, query, nodename, nodeUUID)
	if err != nil {
		return nil, fmt.Errorf("NodeIDsByNodenameAndAuthUUID: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var nodeIDs []string
	for rows.Next() {
		var nodeID sql.NullString
		if err := rows.Scan(&nodeID); err != nil {
			return nil, fmt.Errorf("NodeIDsByNodenameAndAuthUUID scan: %w", err)
		}
		nodeIDs = append(nodeIDs, nodeID.String)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("NodeIDsByNodenameAndAuthUUID rows: %w", err)
	}
	return nodeIDs, nil
}

// SetAuthNodeUUID sets the uuid and the nodename of the auth_node of
// node_id, inserting it when the node has none. It relies on the unique key
// on auth_node.node_id.
func (oDb *DB) SetAuthNodeUUID(ctx context.Context, nodeID, nodename, nodeUUID string) error {
	defer logDuration("SetAuthNodeUUID", time.Now())
	const query = `INSERT INTO auth_node (nodename, uuid, node_id) VALUES (?, ?, ?)
		ON DUPLICATE KEY UPDATE nodename = VALUES(nodename), uuid = VALUES(uuid), updated = NOW()`
	if _, err := oDb.DB.ExecContext(ctx, query, nodename, nodeUUID, nodeID); err != nil {
		return fmt.Errorf("SetAuthNodeUUID: %w", err)
	}
	oDb.SetChange("auth_node")
	return nil
}
