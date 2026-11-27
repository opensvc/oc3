package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// WakeContainer is a container resource whose virtual machine has a given
// machine uuid: the hypervisor node, the service and the rid to start to
// wake the guest node running in it.
type WakeContainer struct {
	NodeID string
	SvcID  string
	RID    string
}

// NodeIDByNodenameAndCluster returns the node_id of the node of the cluster
// with that name, empty when the cluster has none.
func (oDb *DB) NodeIDByNodenameAndCluster(ctx context.Context, nodename, clusterID string) (string, error) {
	var nodeID string
	err := oDb.DB.QueryRowContext(ctx,
		"SELECT node_id FROM nodes WHERE nodename = ? AND cluster_id = ? LIMIT 1", nodename, clusterID).Scan(&nodeID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "", nil
	case err != nil:
		return "", fmt.Errorf("nodeIDByNodenameAndCluster: %w", err)
	}
	return nodeID, nil
}

// NodeMachineUUID returns the machine uuid the agent of the node reports,
// empty when it reports none.
func (oDb *DB) NodeMachineUUID(ctx context.Context, nodeID string) (string, error) {
	var s string
	err := oDb.DB.QueryRowContext(ctx,
		"SELECT COALESCE(machine_uuid, '') FROM nodes WHERE node_id = ?", nodeID).Scan(&s)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "", nil
	case err != nil:
		return "", fmt.Errorf("nodeMachineUUID: %w", err)
	}
	return s, nil
}

// ContainersByMachineUUID returns the container resources reporting that uuid
// as the one of their virtual machine, the uuid key of their resource info.
func (oDb *DB) ContainersByMachineUUID(ctx context.Context, machineUUID string) ([]WakeContainer, error) {
	rows, err := oDb.DB.QueryContext(ctx,
		"SELECT node_id, svc_id, rid FROM resinfo WHERE res_key = 'uuid' AND res_value = ? ORDER BY node_id, svc_id, rid",
		machineUUID)
	if err != nil {
		return nil, fmt.Errorf("containersByMachineUUID: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var l []WakeContainer
	for rows.Next() {
		var (
			w   WakeContainer
			rid sql.NullString
		)
		if err := rows.Scan(&w.NodeID, &w.SvcID, &rid); err != nil {
			return nil, fmt.Errorf("containersByMachineUUID: %w", err)
		}
		w.RID = rid.String
		l = append(l, w)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("containersByMachineUUID: %w", err)
	}
	return l, nil
}

// PendingServiceActionID returns the id of an action of the service queued on
// the node with that command and not done yet, 0 when there is none.
func (oDb *DB) PendingServiceActionID(ctx context.Context, nodeID, svcID, command string) (int64, error) {
	var id int64
	err := oDb.DB.QueryRowContext(ctx,
		"SELECT id FROM action_queue WHERE node_id = ? AND svc_id = ? AND command = ? AND status IN ('Q','N','W','R','S') ORDER BY id DESC LIMIT 1",
		nodeID, svcID, command).Scan(&id)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, nil
	case err != nil:
		return 0, fmt.Errorf("pendingServiceActionID: %w", err)
	}
	return id, nil
}

// PendingNodeActionID returns the id of a node action queued on the node with
// that command and not done yet, 0 when there is none.
func (oDb *DB) PendingNodeActionID(ctx context.Context, nodeID, command string) (int64, error) {
	var id int64
	err := oDb.DB.QueryRowContext(ctx,
		"SELECT id FROM action_queue WHERE node_id = ? AND COALESCE(svc_id, '') = '' AND command = ? AND status IN ('Q','N','W','R','S') ORDER BY id DESC LIMIT 1",
		nodeID, command).Scan(&id)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, nil
	case err != nil:
		return 0, fmt.Errorf("pendingNodeActionID: %w", err)
	}
	return id, nil
}
