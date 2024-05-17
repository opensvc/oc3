package worker

import (
	"fmt"
	"log/slog"

	"github.com/opensvc/oc3/cachekeys"
)

type (
	jobFeedDaemonPing struct {
		*BaseJob

		nodeID     string
		clusterID  string
		callerNode *DBNode

		byObjectID map[string]*DBObject
		byNodeID   map[string]*DBNode
	}
)

func newDaemonPing(nodeID string) *jobFeedDaemonPing {
	return &jobFeedDaemonPing{
		BaseJob: &BaseJob{
			name:   "daemonPing",
			detail: "nodeID: " + nodeID,
		},
		nodeID: nodeID,

		byNodeID:   make(map[string]*DBNode),
		byObjectID: make(map[string]*DBObject),
	}
}

func (d *jobFeedDaemonPing) Operations() []operation {
	return []operation{
		{desc: "daemonPing/dropPending", do: d.dropPending},
		{desc: "daemonPing/dbFetchNodes", do: d.dbFetchNodes},
		{desc: "daemonPing/dbFetchObjects", do: d.dbFetchObjects},
		{desc: "daemonPing/dbPingInstances", do: d.dbPingInstances},
		{desc: "daemonPing/dbPingObjects", do: d.dbPingObjects},
		{desc: "daemonPing/pushFromTableChanges", do: d.pushFromTableChanges},
	}
}

func (d *jobFeedDaemonPing) dropPending() error {
	if err := d.redis.HDel(d.ctx, cachekeys.FeedDaemonPingPendingH, d.nodeID).Err(); err != nil {
		return fmt.Errorf("dropPending: HDEL %s %s: %w", cachekeys.FeedDaemonPingPendingH, d.nodeID, err)
	}
	return nil
}

// dbFetchNodes fetch nodes (that are associated with caller node ID) from database
// and sets d.byNodeID and d.clusterID.
func (d *jobFeedDaemonPing) dbFetchNodes() (err error) {
	var (
		dbNodes []*DBNode
	)
	if dbNodes, err = d.oDb.nodesFromNodeID(d.ctx, d.nodeID); err != nil {
		return fmt.Errorf("dbFetchNodes %s: %w", d.nodeID, err)
	}
	for _, n := range dbNodes {
		d.byNodeID[n.nodeID] = n
	}
	callerNode, ok := d.byNodeID[d.nodeID]
	if !ok {
		return fmt.Errorf("dbFetchNodes source node has been removed")
	}
	d.callerNode = callerNode
	d.clusterID = callerNode.clusterID
	return nil
}

func (d *jobFeedDaemonPing) dbFetchObjects() (err error) {
	var (
		objects []*DBObject
	)
	if objects, err = d.oDb.objectsFromClusterID(d.ctx, d.clusterID); err != nil {
		return fmt.Errorf("dbFetchObjects query node %s (%s) clusterID: %s: %w",
			d.callerNode.nodename, d.nodeID, d.clusterID, err)
	}
	for _, o := range objects {
		d.byObjectID[o.svcID] = o
		slog.Debug(fmt.Sprintf("dbFetchObjects  %s (%s)", o.svcname, o.svcID))
	}
	return nil
}

// dbPingInstances call opensvcDB.instancePingFromNodeID for all db fetched nodes
func (d *jobFeedDaemonPing) dbPingInstances() error {
	for nodeID := range d.byNodeID {
		if ok, err := d.oDb.instancePingFromNodeID(d.ctx, nodeID); err != nil {
			return fmt.Errorf("dbPingInstances: %w", err)
		} else if ok {
			continue
		}
	}
	return nil
}

// dbPingObjects call opensvcDB.objectPing for all db fetched objects
func (d *jobFeedDaemonPing) dbPingObjects() (err error) {
	for objectID, obj := range d.byObjectID {
		objectName := obj.svcname
		if obj.availStatus != "undef" {
			slog.Debug(fmt.Sprintf("ping svc %s %s", objectName, objectID))
			if _, err := d.oDb.objectPing(d.ctx, objectID); err != nil {
				return fmt.Errorf("dbPingObjects can't ping object %s %s: %w", objectName, objectID, err)
			}
		}
	}
	return nil
}

func (d *jobFeedDaemonPing) pushFromTableChanges() error {
	return pushFromTableChanges(d.ctx, d.oDb, d.ev)
}
