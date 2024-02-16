package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"

	"github.com/go-redis/redis/v8"

	"github.com/opensvc/oc3/cache"
)

type (
	DBNode struct {
		nodename  string
		frozen    string
		nodeID    string
		clusterID string
	}

	daemonStatus struct {
		ctx         context.Context
		redis       *redis.Client
		db          *sql.DB
		nodeID      string
		changes     []string
		data        map[string]any
		clusterID   string
		clusterName string
		nodesData   map[string]any
		byNodename  map[string]*DBNode
		byNodeID    map[string]*DBNode
		tableChange map[string]struct{}
	}
)

func (t *Worker) handleDaemonStatus(nodeID string) error {
	d := daemonStatus{
		ctx:         context.Background(),
		redis:       t.Redis,
		db:          t.DB,
		nodeID:      nodeID,
		nodesData:   make(map[string]any),
		byNodename:  make(map[string]*DBNode),
		byNodeID:    make(map[string]*DBNode),
		tableChange: make(map[string]struct{}),
	}
	functions := []func() error{
		d.dropPending,
		d.getChanges,
		d.getData,
		d.dbCheckClusters,
		d.dbFindNodes,
		d.dataToNodeFrozen,
	}
	for _, f := range functions {
		err := f()
		if err != nil {
			return err
		}
	}
	slog.Info(fmt.Sprintf("handleDaemonStatus done: node_id: %s cluster_id: %s, cluster_name: %s changes: %s, byNodes: %#v",
		d.nodeID, d.clusterID, d.clusterName, d.changes, d.byNodename))
	for k, v := range d.byNodename {
		slog.Info(fmt.Sprintf("found node %s: %#v", k, v))
	}
	return nil
}

func (d *daemonStatus) dropPending() error {
	if err := d.redis.HDel(d.ctx, cache.KeyDaemonStatusPending, d.nodeID).Err(); err != nil {
		return fmt.Errorf("dropPening: HDEL %s %s: %w", cache.KeyDaemonStatusPending, d.nodeID, err)
	}
	return nil
}

func (d *daemonStatus) getChanges() error {
	s, err := d.redis.HGet(d.ctx, cache.KeyDaemonStatusChangesHash, d.nodeID).Result()
	if err != nil {
		return fmt.Errorf("getChanges: HGET %s %s: %w", cache.KeyDaemonStatusChangesHash, d.nodeID, err)
	}
	d.changes = strings.Fields(s)
	return nil
}

func (d *daemonStatus) getData() error {
	if b, err := d.redis.HGet(d.ctx, cache.KeyDaemonStatusHash, d.nodeID).Bytes(); err != nil {
		return fmt.Errorf("getChanges: HGET %s %s: %w", cache.KeyDaemonStatusHash, d.nodeID, err)
	} else if err = json.Unmarshal(b, &d.data); err != nil {
		return fmt.Errorf("getChanges: unexpected data from %s %s: %w: %s", cache.KeyDaemonStatusHash, d.nodeID, err)
	}
	if clusterID, ok := d.data["cluster_id"]; !ok {
		return fmt.Errorf("getData: missing mandatory cluster_id for %s", d.nodeID)
	} else if d.clusterID, ok = clusterID.(string); !ok {
		return fmt.Errorf("getData: got empty mandatory cluster_id for %s", d.nodeID)
	}
	if clusterName, ok := d.data["cluster_name"]; !ok {
		return fmt.Errorf("getData: missing mandatory cluster_name for %s", d.nodeID)
	} else if d.clusterName, ok = clusterName.(string); !ok {
		return fmt.Errorf("getData: got empty mandatory cluster_name for %s", d.nodeID)
	}
	return nil
}

func (d *daemonStatus) dbCheckClusters() error {
	// can't assert things here
	return nil
}

func (d *daemonStatus) dbFindNodes() error {
	const queryFindClusterNodesInfo = "SELECT nodename, node_id, node_frozen, cluster_id" +
		" FROM nodes" +
		" WHERE cluster_id = ? AND nodename IN (?"
	nodes, ok := d.data["nodes"].(map[string]any)
	if !ok {
		return fmt.Errorf("getData: missing mandatory nodes for %s", d.nodeID)
	}
	l := make([]string, 0)
	values := []any{d.clusterID}
	for nodename, nodeData := range nodes {
		l = append(l, nodename)
		values = append(values, nodename)
		d.nodesData[nodename] = nodeData
	}
	if len(l) == 0 {
		return fmt.Errorf("getData: empty nodes for %s", d.nodeID)
	}
	query := queryFindClusterNodesInfo
	for i := 1; i < len(l); i++ {
		query += ", ?"
	}
	query += ")"

	rows, err := d.db.QueryContext(d.ctx, query, values...)
	if err != nil {
		return fmt.Errorf("dbFindNodes FindClusterNodesInfo %s [%s]: %w", d.nodeID, nodes, err)
	}
	if rows == nil {
		return fmt.Errorf("dbFindNodes query returns nil rows %s", d.nodeID)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var nodename, node_id, frozen, cluster_id string
		if err := rows.Scan(&nodename, &node_id, &frozen, &cluster_id); err != nil {
			return fmt.Errorf("dbFindNodes FindClusterNodesInfo scan %s: %w", d.nodeID, err)
		}
		found := &DBNode{
			nodename:  nodename,
			frozen:    frozen,
			nodeID:    node_id,
			clusterID: cluster_id,
		}
		d.byNodeID[node_id] = found
		d.byNodename[nodename] = found
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("dbFindNodes FindClusterNodesInfo %s: %w", d.nodeID, err)
	}
	return nil
}

func (d *daemonStatus) dataToNodeFrozen() error {
	for nodeID, dbNode := range d.byNodeID {
		nodename := dbNode.nodename
		if i, ok := d.nodesData[nodename]; ok {
			frozen := "F"
			nodeData := i.(map[string]any)
			switch v := nodeData["frozen"].(type) {
			case int:
				if v > 0 {
					frozen = "T"
				}
			case float64:
				if v > 0 {
					frozen = "T"
				}
			default:
				return fmt.Errorf("dataToNodeFrozen: can't detect node frozen value")
			}
			if frozen != dbNode.frozen {
				const query = "UPDATE nodes SET node_frozen = ? WHERE node_id = ?"
				slog.Info(fmt.Sprintf("dataToNodeFrozen: updating node %s: %s frozen from %s -> %s", nodename, nodeID, dbNode.frozen, frozen))
				if _, err := d.db.ExecContext(d.ctx, query, frozen, nodeID); err != nil {
					return fmt.Errorf("dataToNodeFrozen ExecContext: %w", err)
				}
				d.addTableChange("nodes")
			}
		}
	}
	return nil
}

func (d *daemonStatus) addTableChange(s ...string) {
	for _, table := range s {
		d.tableChange[table] = struct{}{}
	}
}
