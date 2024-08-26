package worker

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/opensvc/oc3/cachekeys"
)

type (
	dataLister interface {
		objectNames() ([]string, error)
		nodeNames() ([]string, error)
	}

	objectInfoer interface {
		appFromObjectName(svcname string, nodes ...string) string
		objectStatus(objectName string) *DBObjStatus
	}

	instancer interface {
		InstanceStatus(objectName string, nodename string) *instanceData
	}

	nodeInfoer interface {
		nodeFrozen(string) (string, error)
	}

	clusterer interface {
		clusterID() (s string, err error)
		clusterName() (s string, err error)
	}

	dataProvider interface {
		dataLister
		clusterer
		nodeInfoer
		objectInfoer
		instancer
	}

	jobFeedDaemonStatus struct {
		*BaseJob

		nodeID      string
		clusterID   string
		clusterName string
		nodeApp     string
		nodeEnv     string
		callerNode  *DBNode

		changes    map[string]struct{}
		rawChanges string
		rawData    []byte

		data dataProvider

		byNodename map[string]*DBNode
		byNodeID   map[string]*DBNode
		nodes      []string

		byObjectName map[string]*DBObject
		byObjectID   map[string]*DBObject

		tableChange map[string]struct{}

		byInstanceName map[string]*DBInstance
		byInstanceID   map[string]*DBInstance
	}

	InstanceID struct {
		nodeID string
		svcID  string
	}
)

func (n *DBNode) String() string {
	return fmt.Sprintf("node: {nodename: %s, node_id: %s, cluster_id: %s, app: %s}", n.nodename, n.nodeID, n.clusterID, n.app)
}

func (i *InstanceID) String() string {
	return fmt.Sprintf("instance id: %s@%s", i.svcID, i.nodeID)
}

func newDaemonStatus(nodeID string) *jobFeedDaemonStatus {
	return &jobFeedDaemonStatus{
		BaseJob: &BaseJob{
			name:   "daemonStatus",
			detail: "nodeID: " + nodeID,

			cachePendingH:   cachekeys.FeedDaemonStatusPendingH,
			cachePendingIDX: nodeID,
		},
		nodeID: nodeID,

		changes: make(map[string]struct{}),

		byNodename: make(map[string]*DBNode),
		byNodeID:   make(map[string]*DBNode),
		nodes:      make([]string, 0),

		byObjectID:   make(map[string]*DBObject),
		byObjectName: make(map[string]*DBObject),

		byInstanceID:   make(map[string]*DBInstance),
		byInstanceName: make(map[string]*DBInstance),
	}
}

func (d *jobFeedDaemonStatus) Operations() []operation {
	return []operation{
		{desc: "daemonStatus/dropPending", do: d.dropPending},
		{desc: "daemonStatus/getChanges", do: d.getChanges},
		{desc: "daemonStatus/getData", do: d.getData},
		{desc: "daemonStatus/dbCheckClusterIDForNodeID", do: d.dbCheckClusterIDForNodeID},
		{desc: "daemonStatus/dbCheckClusters", do: d.dbCheckClusters},
		{desc: "daemonStatus/dbFindNodes", do: d.dbFindNodes},
		{desc: "daemonStatus/dataToNodeFrozen", do: d.dataToNodeFrozen},
		{desc: "daemonStatus/dbFindServices", do: d.dbFindServices},
		{desc: "daemonStatus/dbCreateServices", do: d.dbCreateServices},
		{desc: "daemonStatus/dbFindInstances", do: d.dbFindInstances},
		{desc: "daemonStatus/dbUpdateServices", do: d.dbUpdateServices},
		{desc: "daemonStatus/dbUpdateInstances", do: d.dbUpdateInstances},
		{desc: "daemonStatus/dbPurgeInstances", do: d.dbPurgeInstances},
		{desc: "daemonStatus/dbPurgeServices", do: d.dbPurgeServices},
		{desc: "daemonStatus/cacheObjectsWithoutConfig", do: d.cacheObjectsWithoutConfig},
		{desc: "daemonStatus/pushFromTableChanges", do: d.pushFromTableChanges},
	}
}

func (d *jobFeedDaemonStatus) LogResult() {
	slog.Info(fmt.Sprintf("handleDaemonStatus done for %s", d.byNodeID[d.nodeID]))
	for k, v := range d.byNodename {
		slog.Debug(fmt.Sprintf("found db node %s: %#v", k, v))
	}

	for k, v := range d.byObjectID {
		slog.Debug(fmt.Sprintf("found db object %s: %#v", k, v))
	}

	for k, v := range d.byInstanceName {
		slog.Debug(fmt.Sprintf("found db instance %s: %#v", k, v))
	}
}

func (d *jobFeedDaemonStatus) getChanges() error {
	s, err := d.redis.HGet(d.ctx, cachekeys.FeedDaemonStatusChangesH, d.nodeID).Result()
	if err == nil {
		// TODO: fix possible race:
		// worker iteration 1: pickup changes 'a'
		// listener: read previous changes 'a'
		// listener: merge b => set changes from 'a' to 'a', 'b'
		// listener: ask for new worker iteration 2
		// worker iteration 1: delete changes the 'b' => 'b' change is lost
		// worker iteration 1: ... done
		// worker iteration 2: pickup changes: empty instead of expected 'b'
		if err := d.redis.HDel(d.ctx, cachekeys.FeedDaemonStatusChangesH, d.nodeID).Err(); err != nil {
			return fmt.Errorf("getChanges: HDEL %s %s: %w", cachekeys.FeedDaemonStatusChangesH, d.nodeID, err)
		}
	} else if err != redis.Nil {
		return fmt.Errorf("getChanges: HGET %s %s: %w", cachekeys.FeedDaemonStatusChangesH, d.nodeID, err)
	}
	d.rawChanges = s
	for _, change := range strings.Fields(s) {
		d.changes[change] = struct{}{}
	}
	return nil
}

func (d *jobFeedDaemonStatus) getData() error {
	var (
		err  error
		data map[string]any
	)
	if b, err := d.redis.HGet(d.ctx, cachekeys.FeedDaemonStatusH, d.nodeID).Bytes(); err != nil {
		return fmt.Errorf("getData: HGET %s %s: %w", cachekeys.FeedDaemonStatusH, d.nodeID, err)
	} else if err = json.Unmarshal(b, &data); err != nil {
		return fmt.Errorf("getData: unexpected data from %s %s: %w", cachekeys.FeedDaemonStatusH, d.nodeID, err)
	} else {
		d.rawData = b
		var nilMap map[string]any
		clientVersion := mapToS(data, "", "version")
		switch {
		case strings.HasPrefix(clientVersion, "3."):
			d.data = &daemonDataV3{data: data, cluster: mapToMap(data, nilMap, "data", "cluster")}
		case strings.HasPrefix(clientVersion, "2."):
			d.data = &daemonDataV2{data: mapToMap(data, nilMap, "data")}
		default:
			return fmt.Errorf("no mapper for version %s", clientVersion)
		}
	}
	if d.clusterID, err = d.data.clusterID(); err != nil {
		return fmt.Errorf("getData %s: %w", d.nodeID, err)
	}
	if d.clusterName, err = d.data.clusterName(); err != nil {
		return fmt.Errorf("getData %s: %w", d.nodeID, err)
	}
	return nil
}

func (d *jobFeedDaemonStatus) dbCheckClusterIDForNodeID() error {
	if ok, err := d.oDb.nodeUpdateClusterIDForNodeID(d.ctx, d.nodeID, d.clusterID); err != nil {
		return fmt.Errorf("dbCheckClusterIDForNodeID for %s (%s): %w", d.callerNode.nodename, d.nodeID, err)
	} else if ok {
		slog.Info("dbCheckClusterIDForNodeID change cluster id value")
	}
	return nil
}

func (d *jobFeedDaemonStatus) dbCheckClusters() error {
	if err := d.oDb.updateClustersData(d.ctx, d.clusterName, d.clusterName, string(d.rawData)); err != nil {
		return fmt.Errorf("dbCheckClusters %s (%s): %w", d.nodeID, d.clusterID, err)
	}
	return nil
}

func (d *jobFeedDaemonStatus) dbFindNodes() (err error) {
	var (
		nodes   []string
		dbNodes []*DBNode
	)

	// search caller node from its node_id: we can't trust yet search from
	// d.data.nodeNames() because initial push daemon status may omit caller node.
	if callerNode, err := d.oDb.nodeByNodeID(d.ctx, d.nodeID); err != nil {
		return fmt.Errorf("dbFindNodes nodeByNodeID %s: %s", d.nodeID, err)
	} else if callerNode == nil {
		return fmt.Errorf("dbFindNodes can't find caller node %s", d.nodeID)
	} else {
		d.callerNode = callerNode
		d.nodeApp = callerNode.app
		d.nodeEnv = callerNode.nodeEnv
		d.byNodeID[d.nodeID] = callerNode
		d.byNodename[callerNode.nodename] = callerNode
	}

	// search all cluster nodes
	nodes, err = d.data.nodeNames()
	if err != nil {
		return fmt.Errorf("getData %s: %w", d.nodeID, err)
	}
	if len(nodes) == 0 {
		return fmt.Errorf("dbFindNodes: empty nodes for %s", d.nodeID)
	}
	if dbNodes, err = d.oDb.nodesFromClusterIDWithNodenames(d.ctx, d.clusterID, nodes); err != nil {
		return fmt.Errorf("dbFindNodes %s [%s]: %w", nodes, d.nodeID, err)
	}
	for _, n := range dbNodes {
		if n.nodeID == d.nodeID {
			// already processed
			continue
		}
		d.byNodeID[n.nodeID] = n
		d.byNodename[n.nodename] = n
	}
	d.nodes = make([]string, len(d.byNodename))
	var i = 0
	for nodename := range d.byNodename {
		d.nodes[i] = nodename
		i++
	}
	slog.Info(fmt.Sprintf("handleDaemonStatus run details: %s changes: [%s]", d.callerNode, d.rawChanges))
	return nil
}

func (d *jobFeedDaemonStatus) dataToNodeFrozen() error {
	for nodeID, dbNode := range d.byNodeID {
		nodename := dbNode.nodename
		frozen, err := d.data.nodeFrozen(nodename)
		if err != nil {
			if nodeID == d.nodeID {
				// accept missing caller node in initial data
				continue
			} else {
				return fmt.Errorf("dataToNodeFrozen %s (%s): %w", nodename, nodeID, err)
			}
		}
		if frozen != dbNode.frozen {
			slog.Info(fmt.Sprintf("dataToNodeFrozen: updating node %s: %s frozen from %s -> %s", nodename, nodeID, dbNode.frozen, frozen))
			if err := d.oDb.nodeUpdateFrozen(d.ctx, nodeID, frozen); err != nil {
				return fmt.Errorf("dataToNodeFrozen node %s (%s): %w", nodename, dbNode.nodeID, err)
			}
		}
	}
	return nil
}

func (d *jobFeedDaemonStatus) dbFindServices() error {
	var (
		objects []*DBObject
	)
	objectNames, err := d.data.objectNames()
	if err != nil {
		return fmt.Errorf("dbFindServices %s: %w", d.nodeID, err)
	}
	if len(objectNames) == 0 {
		slog.Info(fmt.Sprintf("dbFindServices: no services for %s", d.nodeID))
		return nil
	}
	if objects, err = d.oDb.objectsFromClusterIDAndObjectNames(d.ctx, d.clusterID, objectNames); err != nil {
		return fmt.Errorf("dbFindServices query nodeID: %s clusterID: %s [%s]: %w", d.nodeID, d.clusterID, objectNames, err)
	}
	for _, o := range objects {
		d.byObjectName[o.svcname] = o
		d.byObjectID[o.svcID] = o
		slog.Debug(fmt.Sprintf("dbFindServices %s (%s)", o.svcname, o.svcID))
	}
	return nil
}

func (d *jobFeedDaemonStatus) dbFindInstances() error {
	var (
		objectIDs = make([]string, 0)
	)
	if len(d.byObjectID) == 0 {
		return nil
	}
	for objectID := range d.byObjectID {
		objectIDs = append(objectIDs, objectID)
	}
	instances, err := d.oDb.instancesFromObjectIDs(d.ctx, objectIDs...)
	if err != nil {
		return fmt.Errorf("dbFindInstances: %w", err)
	}

	for _, mon := range instances {
		if n, ok := d.byNodeID[mon.nodeID]; ok {
			// Only pickup instances from known nodes
			if s, ok := d.byObjectID[mon.svcID]; ok {
				// Only pickup instances from known objects
				d.byInstanceName[s.svcname+"@"+n.nodename] = mon
				d.byInstanceID[s.svcID+"@"+n.nodeID] = mon
				slog.Debug(fmt.Sprintf("dbFindInstances found %s@%s (%s@%s)",
					s.svcname, n.nodename, s.svcID, n.nodeID))
			}
		}
	}
	return nil
}

// dbCreateServices creates missing services
func (d *jobFeedDaemonStatus) dbCreateServices() error {
	objectNames, err := d.data.objectNames()
	if err != nil {
		return fmt.Errorf("dbCreateServices: %w", err)
	}
	missing := make([]string, 0)
	for _, objectName := range objectNames {
		if _, ok := d.byObjectName[objectName]; ok {
			continue
		}
		missing = append(missing, objectName)
	}
	if len(missing) == 0 {
		return nil
	}
	slog.Debug(fmt.Sprintf("dbCreateServices: need create services: %v", missing))
	for _, objectName := range missing {
		app := d.data.appFromObjectName(objectName, d.nodes...)
		slog.Debug(fmt.Sprintf("dbCreateServices: creating service %s with app %s", objectName, app))
		obj, err := d.oDb.objectCreate(d.ctx, objectName, d.clusterID, app, d.byNodeID[d.nodeID])
		if err != nil {
			return fmt.Errorf("dbCreateServices objectCreate %s: %w", objectName, err)
		}
		slog.Debug(fmt.Sprintf("created service %s with app %s new id: %s", objectName, app, obj.svcID))
		d.byObjectName[objectName] = obj
		d.byObjectName[obj.svcID] = obj
	}
	return nil
}

func (d *jobFeedDaemonStatus) dbUpdateServices() error {
	for objectID, obj := range d.byObjectID {
		objectName := obj.svcname
		_, isChanged := d.changes[objectName]
		// freshly created services have availStatus "undef" and needs full update
		// even if not present in changes
		if !isChanged && obj.availStatus != "undef" {
			slog.Debug(fmt.Sprintf("ping svc %s %s", objectName, objectID))
			if _, err := d.oDb.objectPing(d.ctx, objectID); err != nil {
				return fmt.Errorf("dbUpdateServices can't ping object %s %s: %w", objectName, objectID, err)
			}
		} else {
			oStatus := d.data.objectStatus(objectName)
			if oStatus != nil {
				slog.Debug(fmt.Sprintf("update svc log %s %s %#v", objectName, objectID, oStatus))
				if err := d.oDb.objectUpdateLog(d.ctx, objectID, oStatus.availStatus); err != nil {
					return fmt.Errorf("dbUpdateServices can't update object log %s %s: %w", objectName, objectID, err)
				}
				slog.Debug(fmt.Sprintf("update svc %s %s %#v", objectName, objectID, *oStatus))
				if err := d.oDb.objectUpdateStatus(d.ctx, objectID, oStatus); err != nil {
					return fmt.Errorf("dbUpdateServices can't update object %s %s: %w", objectName, objectID, err)
				}
				if d.byObjectID[objectID].availStatus != oStatus.availStatus {
					slog.Debug(fmt.Sprintf("dbUpdateServices %s avail status %s -> %s", objectName, d.byObjectID[objectID].availStatus, oStatus.availStatus))
				}
				// refresh local cache
				d.byObjectID[objectID].DBObjStatus = *oStatus
			}
		}
	}
	return nil
}

func (d *jobFeedDaemonStatus) dbUpdateInstances() error {
	for objectName, obj := range d.byObjectName {
		beginObj := time.Now()
		objID := obj.svcID
		instanceMonitorStates := make(map[string]bool)
		for nodeID, node := range d.byNodeID {
			beginInstance := time.Now()
			if node == nil {
				return fmt.Errorf("dbUpdateInstances unexpected nil value for byNodeID(%s)", nodeID)
			}
			nodename := node.nodename
			iStatus := d.data.InstanceStatus(objectName, nodename)
			if iStatus == nil {
				continue
			}
			_, isChanged := d.changes[objectName+"@"+nodename]
			if !isChanged && obj.availStatus != "undef" {
				slog.Debug(fmt.Sprintf("ping instance %s@%s", objectName, nodename))
				changes, err := d.oDb.instancePing(d.ctx, objID, nodeID)
				if err != nil {
					return fmt.Errorf("dbUpdateInstances can't ping instance %s@%s: %w", objectName, nodename, err)
				} else if changes {
					// the instance already existed, and the updated tstamp has been refreshed
					// skip the inserts/updates
					continue
				}
			}
			instanceMonitorStates[iStatus.monSmonStatus] = true
			if iStatus.encap == nil {
				subNodeID, _, _, err := d.oDb.translateEncapNodename(d.ctx, objID, nodeID)
				if err != nil {
					return err
				}
				if subNodeID != "" && subNodeID != nodeID {
					slog.Debug(fmt.Sprintf("dbUpdateInstances skip for %s@%s subNodeID:%s vs nodeID: %subNodeID", objectName, nodename, subNodeID, nodeID))
					continue
				}
				if iStatus.resources == nil {
					// scaler or wrapper, for example
					if err := d.oDb.instanceDeleteStatus(d.ctx, objID, nodeID); err != nil {
						return fmt.Errorf("dbUpdateInstances delete status %s@%s: %w", objID, nodeID, err)
					}
					if err := d.oDb.instanceResourcesDelete(d.ctx, objID, nodeID); err != nil {
						return fmt.Errorf("dbUpdateInstances delete resources %s@%s: %w", objID, nodeID, err)
					}
				} else {
					// set iStatus svcID and nodeID for db update
					iStatus.svcID = objID
					iStatus.nodeID = nodeID
					if err := d.instanceStatusUpdate(objectName, nodename, iStatus); err != nil {
						return fmt.Errorf("dbUpdateInstances update status %s@%s (%s@%s): %w", objectName, nodename, objID, nodeID, err)
					}
					resourceObsoleteAt := time.Now()
					if err := d.instanceResourceUpdate(objectName, nodename, iStatus); err != nil {
						return fmt.Errorf("dbUpdateInstances update resource %s@%s (%s@%s): %w", objectName, nodename, objID, nodeID, err)
					}
					slog.Debug(fmt.Sprintf("dbUpdateInstances deleting obsolete resources %s@%s", objectName, nodename))
					if err := d.oDb.instanceResourcesDeleteObsolete(d.ctx, objID, nodeID, resourceObsoleteAt); err != nil {
						return fmt.Errorf("dbUpdateInstances delete obsolete resources %s@%s: %w", objID, nodeID, err)
					}
				}
			} else {
				if iStatus.resources == nil {
					// scaler or wrapper, for example
					if err := d.oDb.instanceDeleteStatus(d.ctx, objID, nodeID); err != nil {
						return fmt.Errorf("dbUpdateInstances delete status %s@%s: %w", objID, nodeID, err)
					}
					if err := d.oDb.instanceResourcesDelete(d.ctx, objID, nodeID); err != nil {
						return fmt.Errorf("dbUpdateInstances delete resources %s@%s: %w", objID, nodeID, err)
					}
				} else {
					resourceObsoleteAt := time.Now()
					for _, containerStatus := range iStatus.Containers() {
						if containerStatus.fromOutsideStatus == "up" {
							slog.Debug(fmt.Sprintf("dbUpdateInstances nodeContainerUpdateFromParentNode %s@%s encap hostname %s",
								objID, nodeID, containerStatus.monVmName))
							if err := d.oDb.nodeContainerUpdateFromParentNode(d.ctx, containerStatus.monVmName, obj.app, node); err != nil {
								return fmt.Errorf("dbUpdateInstances nodeContainerUpdateFromParentNode %s@%s encap hostname %s: %w",
									objID, nodeID, containerStatus.monVmName, err)
							}
						}

						if err := d.instanceStatusUpdate(objID, nodeID, containerStatus); err != nil {
							return fmt.Errorf("dbUpdateInstances update container %s %s@%s (%s@%s): %w",
								containerStatus.monVmName, objID, nodeID, objectName, nodename, err)
						}
					}
					slog.Debug(fmt.Sprintf("dbUpdateInstances deleting obsolete container resources %s@%s", objectName, nodename))
					if err := d.oDb.instanceResourcesDeleteObsolete(d.ctx, objID, nodeID, resourceObsoleteAt); err != nil {
						return fmt.Errorf("dbUpdateInstances delete obsolete container resources %s@%s: %w", objID, nodeID, err)
					}
				}
			}
			if err := d.oDb.dashboardInstanceFrozenUpdate(d.ctx, objID, nodeID, obj.env, iStatus.monFrozen > 0); err != nil {
				return fmt.Errorf("dbUpdateInstances update dashboard instance frozen %s@%s (%s@%s): %w", objectName, nodename, objID, nodeID, err)
			}
			if err := d.oDb.dashboardDeleteInstanceNotUpdated(d.ctx, objID, nodeID); err != nil {
				return fmt.Errorf("dbUpdateInstances update dashboard instance not updated %s@%s (%s@%s): %w", objectName, nodename, objID, nodeID, err)
			}
			// TODO: verify if we need a placement non optimal alert for object/instance
			//     om2 has: monitor.services.'<path>'.placement = non-optimal
			//     om3 has: cluster.object.<path>.placement_state = non-optimal
			//				cluster.node.<node>.instance.<path>.monitor.is_ha_leader
			//				cluster.node.<node>.instance.<path>.monitor.is_leader
			//     collector v2 calls update_dash_service_not_on_primary (broken since no DEFAULT.autostart_node values)

			slog.Debug(fmt.Sprintf("STAT: dbUpdateInstances instance duration %s@%s %s", objectName, nodename, time.Now().Sub(beginInstance)))
		}
		beginObjDash := time.Now()
		if len(instanceMonitorStates) == 1 && instanceMonitorStates["idle"] {
			var remove bool

			remove = slices.Contains([]string{"up", "n/a"}, obj.availStatus)
			if err := d.updateDashboardObject(obj, remove, &DashboardObjectUnavailable{obj: obj}); err != nil {
				return fmt.Errorf("dbUpdateInstances on %s (%s): %w", objID, objectName, err)
			}

			remove = slices.Contains([]string{"optimal", "n/a"}, obj.placement)
			if err := d.updateDashboardObject(obj, remove, &DashboardObjectPlacement{obj: obj}); err != nil {
				return fmt.Errorf("dbUpdateInstances on %s (%s): %w", objID, objectName, err)
			}

			remove = slices.Contains([]string{"up", "n/a"}, obj.availStatus) && slices.Contains([]string{"up", "n/a"}, obj.overallStatus)
			if err := d.updateDashboardObject(obj, remove, &DashboardObjectDegraded{obj: obj}); err != nil {
				return fmt.Errorf("dbUpdateInstances on %s (%s): %w", objID, objectName, err)
			}

			if err := d.oDb.dashboardUpdateObjectFlexStarted(d.ctx, obj); err != nil {
				return fmt.Errorf("dbUpdateInstances %s (%s): %w", objID, objectName, err)
			}
			// Dropped feature: update_dash_flex_cpu
		}
		slog.Debug(fmt.Sprintf("STAT: dbUpdateInstances object dashboard duration %s %s", objectName, time.Now().Sub(beginObjDash)))
		slog.Debug(fmt.Sprintf("STAT: dbUpdateInstances object duration %s %s", objectName, time.Now().Sub(beginObj)))
	}

	return nil
}

func (d *jobFeedDaemonStatus) instanceResourceUpdate(objName string, nodename string, iStatus *instanceData) error {
	for _, res := range iStatus.InstanceResources() {
		slog.Debug(fmt.Sprintf("updating instance resource %s@%s %s (%s@%s)", objName, nodename, res.rid, iStatus.svcID, iStatus.nodeID))
		if err := d.oDb.instanceResourceUpdate(d.ctx, res); err != nil {
			return fmt.Errorf("update resource %s: %w", res.rid, err)
		}
		slog.Debug(fmt.Sprintf("updating instance resource log %s@%s %s (%s@%s)", objName, nodename, res.rid, iStatus.svcID, iStatus.nodeID))
		if err := d.oDb.instanceResourceLogUpdate(d.ctx, res); err != nil {
			return fmt.Errorf("update resource log %s: %w", res.rid, err)
		}
	}
	return nil
}

func (d *jobFeedDaemonStatus) instanceStatusUpdate(objName string, nodename string, iStatus *instanceData) error {
	slog.Debug(fmt.Sprintf("updating instance status %s@%s (%s@%s)", objName, nodename, iStatus.svcID, iStatus.nodeID))
	if err := d.oDb.instanceStatusUpdate(d.ctx, &iStatus.DBInstanceStatus); err != nil {
		return fmt.Errorf("update instance status: %w", err)
	}
	slog.Debug(fmt.Sprintf("instanceStatusUpdate updating status log %s@%s (%s@%s)", objName, nodename, iStatus.svcID, iStatus.nodeID))
	err := d.oDb.instanceStatusLogUpdate(d.ctx, &iStatus.DBInstanceStatus)
	if err != nil {
		return fmt.Errorf("update instance status log: %w", err)
	}
	return nil
}

func (d *jobFeedDaemonStatus) dbPurgeInstances() error {
	var nodeIDs, objectNames []string
	for objectName := range d.byObjectName {
		objectNames = append(objectNames, objectName)
	}
	for nodeID := range d.byNodeID {
		nodeIDs = append(nodeIDs, nodeID)
	}
	instanceIDs, err := d.oDb.getOrphanInstances(d.ctx, nodeIDs, objectNames)
	if err != nil {
		return fmt.Errorf("dbPurgeInstances: getOrphanInstances: %w", err)
	}
	for _, instanceID := range instanceIDs {
		if err1 := d.oDb.purgeInstances(d.ctx, instanceID); err1 != nil {
			err = errors.Join(err, fmt.Errorf("purge instance %v: %w", instanceID, err1))
		}
	}
	if err != nil {
		return fmt.Errorf("dbPurgeInstances: %w", err)
	}
	return nil
}

func (d *jobFeedDaemonStatus) dbPurgeServices() error {
	objectIDs, err := d.oDb.objectIDsFromClusterIDWithPurgeTag(d.ctx, d.clusterID)
	if err != nil {
		err = fmt.Errorf("dbPurgeServices objectIDsFromClusterIDWithPurgeTag: %w", err)
		return err
	}
	for _, objectID := range objectIDs {
		if err1 := d.oDb.purgeTablesFromObjectID(d.ctx, objectID); err1 != nil {
			err = errors.Join(err, fmt.Errorf("purge object %s: %w", objectID, err1))
		}
	}
	if err != nil {
		return fmt.Errorf("dbPurgeServices: %w", err)
	}
	return nil
}

// cacheObjectsWithoutConfig populate FeedObjectConfigForClusterIDH with names of objects without config
func (d *jobFeedDaemonStatus) cacheObjectsWithoutConfig() error {
	objects, err := d.populateFeedObjectConfigForClusterIDH(d.clusterID, d.byObjectID)
	if len(objects) > 0 {
		slog.Info(fmt.Sprintf("daemonStatus nodeID: %s need object config: %s", d.nodeID, objects))
	}
	return err
}

func logDuration(s string, begin time.Time) {
	slog.Debug(fmt.Sprintf("STAT: %s elapse: %s", s, time.Now().Sub(begin)))
}

func logDurationInfo(s string, begin time.Time) {
	slog.Info(fmt.Sprintf("STAT: %s elapse: %s", s, time.Now().Sub(begin)))
}
