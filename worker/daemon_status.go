package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/opensvc/oc3/cache"
)

type (
	DBNode struct {
		nodename  string
		frozen    string
		nodeID    string
		clusterID string
		app       string
		nodeEnv   string
	}

	DBObject struct {
		svcname   string
		svcID     string
		clusterID string

		DBObjStatus

		env string
	}

	DBInstance struct {
		svcID  string
		nodeID string
		Frozen int
	}

	dataLister interface {
		objectNames() ([]string, error)
		nodeNames() ([]string, error)
	}

	objectInfoer interface {
		appFromObjectName(svcname string, nodes ...string) string
		objectStatus(objectName string) *DBObjStatus
	}

	instancer interface {
		InstanceStatus(objectName string, nodename string) *instanceStatus
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

	daemonStatus struct {
		ctx   context.Context
		redis *redis.Client
		db    DBOperater
		oDb   *opensvcDB
		ev    EventPublisher

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

func (t *Worker) handleDaemonStatus(nodeID string) error {
	defer logDurationInfo(fmt.Sprintf("handleDaemonStatus %s with tx %v", nodeID, t.WithTx), time.Now())
	slog.Info(fmt.Sprintf("handleDaemonStatus starting for node_id %s", nodeID))
	ctx := context.Background()

	d := daemonStatus{
		ctx:    ctx,
		redis:  t.Redis,
		nodeID: nodeID,
		ev:     t.Ev,

		changes: make(map[string]struct{}),

		byNodename: make(map[string]*DBNode),
		byNodeID:   make(map[string]*DBNode),

		byObjectID:   make(map[string]*DBObject),
		byObjectName: make(map[string]*DBObject),

		byInstanceID:   make(map[string]*DBInstance),
		byInstanceName: make(map[string]*DBInstance),
	}

	switch t.WithTx {
	case true:
		if tx, err := t.DB.BeginTx(ctx, nil); err != nil {
			return err
		} else {
			d.db = tx
			d.oDb = &opensvcDB{db: tx, tChanges: make(map[string]struct{})}
		}
	case false:
		d.db = t.DB
		d.oDb = &opensvcDB{db: t.DB, tChanges: make(map[string]struct{})}
	}

	chain := func(f ...func() error) error {
		for _, f := range f {
			err := f()
			if err != nil {
				return err
			}
		}
		return nil
	}

	err := chain(d.dropPending,
		d.getChanges,
		d.getData,
		d.dbCheckClusterIDForNodeID,
		d.dbCheckClusters,
		d.dbFindNodes,
		d.dataToNodeFrozen,
		d.dbFindServices,
		d.dbCreateServices,
		d.dbFindInstance,
		d.dbUpdateServices,
		d.dbUpdateInstance,
		d.dbPurgeInstance,
		d.dbPurgeService,
		d.pushFromTableChanges,
	)
	if err != nil {
		if tx, ok := d.db.(DBTxer); ok {
			slog.Debug("handleDaemonStatus rollback on error")
			if err := tx.Rollback(); err != nil {
				slog.Error(fmt.Sprintf("handleDaemonStatus rollback failed: %s", err))
			}
		}
		return fmt.Errorf("handleDaemonStatus node_id %s cluster_id %s: %w", nodeID, d.clusterID, err)
	} else if tx, ok := d.db.(DBTxer); ok {
		slog.Debug("handleDaemonStatus commit")
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("handleDaemonStatus commit: %w", err)
		}
	}
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
	return nil
}

func (d *daemonStatus) dropPending() error {
	defer logDuration("dropPending", time.Now())
	if err := d.redis.HDel(d.ctx, cache.KeyDaemonStatusPending, d.nodeID).Err(); err != nil {
		return fmt.Errorf("dropPening: HDEL %s %s: %w", cache.KeyDaemonStatusPending, d.nodeID, err)
	}
	return nil
}

func (d *daemonStatus) getChanges() error {
	defer logDuration("getChanges", time.Now())
	s, err := d.redis.HGet(d.ctx, cache.KeyDaemonStatusChangesHash, d.nodeID).Result()
	if err == nil {
		// TODO: fix possible race:
		// worker iteration 1: pickup changes 'a'
		// listener: read previous changes 'a'
		// listener: merge b => set changes from 'a' to 'a', 'b'
		// listener: ask for new worker iteration 2
		// worker iteration 1: delete changes the 'b' => 'b' change is lost
		// worker iteration 1: ... done
		// worker iteration 2: pickup changes: empty instead of expected 'b'
		if err := d.redis.HDel(d.ctx, cache.KeyDaemonStatusChangesHash, d.nodeID).Err(); err != nil {
			return fmt.Errorf("getChanges: HDEL %s %s: %w", cache.KeyDaemonStatusChangesHash, d.nodeID, err)
		}
	} else if err != redis.Nil {
		return fmt.Errorf("getChanges: HGET %s %s: %w", cache.KeyDaemonStatusChangesHash, d.nodeID, err)
	}
	d.rawChanges = s
	for _, change := range strings.Fields(s) {
		d.changes[change] = struct{}{}
	}
	return nil
}

func (d *daemonStatus) getData() error {
	defer logDuration("getData", time.Now())
	var (
		err  error
		data map[string]any
	)
	if b, err := d.redis.HGet(d.ctx, cache.KeyDaemonStatusHash, d.nodeID).Bytes(); err != nil {
		return fmt.Errorf("getChanges: HGET %s %s: %w", cache.KeyDaemonStatusHash, d.nodeID, err)
	} else if err = json.Unmarshal(b, &data); err != nil {
		return fmt.Errorf("getChanges: unexpected data from %s %s: %w", cache.KeyDaemonStatusHash, d.nodeID, err)
	} else {
		d.rawData = b
		d.data = &daemonDataV2{data: data}
	}
	if d.clusterID, err = d.data.clusterID(); err != nil {
		return fmt.Errorf("getData %s: %w", d.nodeID, err)
	}
	if d.clusterName, err = d.data.clusterName(); err != nil {
		return fmt.Errorf("getData %s: %w", d.nodeID, err)
	}
	return nil
}

func (d *daemonStatus) dbCheckClusterIDForNodeID() error {
	defer logDuration("dbCheckClusterIDForNodeID", time.Now())
	const querySearch = "SELECT cluster_id FROM nodes WHERE node_id = ? and cluster_id = ?"
	const queryUpdate = "UPDATE nodes SET cluster_id = ? WHERE node_id = ?"
	row := d.db.QueryRowContext(d.ctx, querySearch, d.nodeID, d.clusterID)
	var s string
	err := row.Scan(&s)
	switch err {
	case nil:
		return nil
	case sql.ErrNoRows:
		slog.Info(fmt.Sprintf("dbCheckClusterIDForNodeID update cluster for %s", d.nodeID))
		if _, err := d.db.ExecContext(d.ctx, queryUpdate, d.clusterID, d.nodeID); err != nil {
			return fmt.Errorf("dbCheckClusterIDForNodeID can't update cluster_id for node %s: %w", d.nodeID, err)
		}
		return nil
	default:
		return fmt.Errorf("dbCheckClusterIDForNodeID can't verify cluster_id for node %s: %w", d.nodeID, err)
	}
}

func (d *daemonStatus) dbCheckClusters() error {
	defer logDuration("dbCheckClusters", time.Now())
	// TODO: verify if still needed, we can't assert things here
	// +--------------+--------------+------+-----+---------+----------------+
	// | Field        | Type         | Null | Key | Default | Extra          |
	// +--------------+--------------+------+-----+---------+----------------+
	// | id           | int(11)      | NO   | PRI | NULL    | auto_increment |
	// | cluster_id   | char(36)     | YES  | UNI |         |                |
	// | cluster_name | varchar(128) | NO   |     | NULL    |                |
	// | cluster_data | longtext     | YES  |     | NULL    |                |
	// +--------------+--------------+------+-----+---------+----------------+
	const query = "" +
		"INSERT INTO clusters (cluster_name, cluster_data, cluster_id) VALUES (?, ?, ?)" +
		" ON DUPLICATE KEY UPDATE cluster_name = ?, cluster_data = ?"
	clusterData := string(d.rawData)
	if _, err := d.db.ExecContext(d.ctx, query, d.clusterName, clusterData, d.clusterID, d.clusterName, clusterData); err != nil {
		return fmt.Errorf("dbCheckClusters can't update clusters nodeID: %s clusterID: %s: %w", d.nodeID, d.clusterID, err)
	}
	return nil
}

func (d *daemonStatus) dbFindNodes() error {
	defer logDuration("dbFindNodes", time.Now())
	const queryFindClusterNodesInfo = "" +
		"SELECT nodename, node_id, node_frozen, cluster_id, app, node_env" +
		" FROM nodes" +
		" WHERE cluster_id = ? AND nodename IN (?"
	nodes, err := d.data.nodeNames()
	if err != nil {
		return fmt.Errorf("getData %s: %w", d.nodeID, err)
	}
	l := make([]string, 0)
	values := []any{d.clusterID}
	for _, nodename := range nodes {
		l = append(l, nodename)
		values = append(values, nodename)
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
		var nodename, nodeID, frozen, clusterID, app, nodeEnv string
		if err := rows.Scan(&nodename, &nodeID, &frozen, &clusterID, &app, &nodeEnv); err != nil {
			return fmt.Errorf("dbFindNodes FindClusterNodesInfo scan %s: %w", d.nodeID, err)
		}
		found := &DBNode{
			nodename:  nodename,
			frozen:    frozen,
			nodeID:    nodeID,
			clusterID: clusterID,
			app:       app,
			nodeEnv:   nodeEnv,
		}
		d.byNodeID[nodeID] = found
		d.byNodename[nodename] = found
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("dbFindNodes FindClusterNodesInfo %s: %w", d.nodeID, err)
	}
	callerNode, ok := d.byNodeID[d.nodeID]
	if !ok {
		return fmt.Errorf("dbFindNodes source node has been removed")
	}
	d.callerNode = callerNode
	d.nodeApp = callerNode.app
	d.nodeEnv = callerNode.nodeEnv
	d.nodes = make([]string, 0, len(d.byNodename))
	for nodename := range d.byNodename {
		d.nodes = append(d.nodes, nodename)
	}
	slog.Info(fmt.Sprintf("handleDaemonStatus run details: %s changes: [%s]", callerNode, d.rawChanges))
	return nil
}

func (d *daemonStatus) dataToNodeFrozen() error {
	defer logDuration("dataToNodeFrozen", time.Now())
	for nodeID, dbNode := range d.byNodeID {
		nodename := dbNode.nodename
		frozen, err := d.data.nodeFrozen(nodename)
		if err != nil {
			return fmt.Errorf("dataToNodeFrozen %s: %w", nodename, err)
		}
		if frozen != dbNode.frozen {
			const query = "UPDATE nodes SET node_frozen = ? WHERE node_id = ?"
			slog.Info(fmt.Sprintf("dataToNodeFrozen: updating node %s: %s frozen from %s -> %s", nodename, nodeID, dbNode.frozen, frozen))
			if _, err := d.db.ExecContext(d.ctx, query, frozen, nodeID); err != nil {
				return fmt.Errorf("dataToNodeFrozen ExecContext: %w", err)
			}
			d.oDb.tableChange("nodes")
		}
	}
	return nil
}

func (d *daemonStatus) dbFindServices() error {
	defer logDuration("dbFindServices", time.Now())
	const queryFindServicesInfo = "" +
		"SELECT svcname, svc_id, cluster_id, svc_availstatus, svc_env" +
		" FROM services" +
		" WHERE cluster_id = ? AND svcname IN (?"
	objectNames, err := d.data.objectNames()
	if err != nil {
		return fmt.Errorf("dbFindServices %s: %w", d.nodeID, err)
	}
	l := make([]string, 0)
	values := []any{d.clusterID}
	for _, objectName := range objectNames {
		l = append(l, objectName)
		values = append(values, objectName)
	}
	if len(l) == 0 {
		slog.Info(fmt.Sprintf("dbFindServices: no services for %s", d.nodeID))
		return nil
	}
	query := queryFindServicesInfo
	for i := 1; i < len(l); i++ {
		query += ", ?"
	}
	query += ")"

	rows, err := d.db.QueryContext(d.ctx, query, values...)
	if err != nil {
		return fmt.Errorf("dbFindServices query %s cluster_id: %s [%s]: %w", d.nodeID, d.clusterID, l, err)
	}
	if rows == nil {
		return fmt.Errorf("dbFindServices query returns nil rows %s", d.nodeID)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var o DBObject
		if err := rows.Scan(&o.svcname, &o.svcID, &o.clusterID, &o.availStatus, &o.env); err != nil {
			return fmt.Errorf("dbFindServices scan %s: %w", d.nodeID, err)
		}
		d.byObjectName[o.svcname] = &o
		d.byObjectID[o.svcID] = &o
		slog.Debug(fmt.Sprintf("dbFindServices %s (%s)", o.svcname, o.svcID))
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("dbFindServices FindClusterNodesInfo %s: %w", d.nodeID, err)
	}
	return nil
}

func (d *daemonStatus) dbFindInstance() error {
	defer logDuration("dbFindInstance", time.Now())
	const querySelect = "" +
		"SELECT svc_id, node_id, mon_frozen" +
		" FROM svcmon" +
		" WHERE svc_id IN (?"

	var values []any
	for svcID := range d.byObjectID {
		values = append(values, svcID)
	}
	if len(values) == 0 {
		return nil
	}
	query := querySelect
	for i := 1; i < len(values); i++ {
		query += ", ?"
	}
	query += ")"

	rows, err := d.db.QueryContext(d.ctx, query, values...)
	if err != nil {
		return fmt.Errorf("dbFindInstance query svcIDs: [%s]: %w", values, err)
	}
	if rows == nil {
		return fmt.Errorf("dbFindInstance query returns nil rows")
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var o DBInstance
		if err := rows.Scan(&o.svcID, &o.nodeID, &o.Frozen); err != nil {
			return fmt.Errorf("dbFindInstance scan %s: %w", d.nodeID, err)
		}
		if n, ok := d.byNodeID[o.nodeID]; ok {
			// Only pickup instances from known nodes
			if s, ok := d.byObjectID[o.svcID]; ok {
				// Only pickup instances from known objects
				d.byInstanceName[s.svcname+"@"+n.nodename] = &o
				d.byInstanceID[s.svcID+"@"+n.nodeID] = &o
				slog.Debug(fmt.Sprintf("dbFindInstance found %s@%s (%s@%s)",
					s.svcname, n.nodename, s.svcID, n.nodeID))
			}
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("dbFindInstance query rows: %w", err)
	}
	return nil
}

// dbCreateServices creates missing services
func (d *daemonStatus) dbCreateServices() error {
	defer logDuration("dbCreateServices", time.Now())
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
		obj, err := d.oDb.createNewObject(d.ctx, objectName, d.clusterID, app, d.byNodeID[d.nodeID])
		if err != nil {
			return fmt.Errorf("dbCreateServices createNewObject %s: %w", objectName, err)
		}
		slog.Debug(fmt.Sprintf("created service %s with app %s new id: %s", objectName, app, obj.svcID))
		d.byObjectName[objectName] = obj
		d.byObjectName[obj.svcID] = obj
	}
	return nil
}

func (d *daemonStatus) dbUpdateServices() error {
	defer logDuration("dbUpdateServices", time.Now())
	for objectID, obj := range d.byObjectID {
		objectName := obj.svcname
		_, isChanged := d.changes[objectName]
		// freshly created services have availStatus "undef" and needs full update
		// even if not present in changes
		if !isChanged && obj.availStatus != "undef" {
			slog.Debug(fmt.Sprintf("ping svc %s %s", objectName, objectID))
			if _, err := d.oDb.pingObject(d.ctx, objectID); err != nil {
				return fmt.Errorf("dbUpdateServices can't ping object %s %s: %w", objectName, objectID, err)
			}
		} else {
			oStatus := d.data.objectStatus(objectName)
			if oStatus != nil {
				slog.Debug(fmt.Sprintf("update svc log %s %s %#v", objectName, objectID, oStatus))
				if err := d.oDb.updateObjectLog(d.ctx, objectID, oStatus.availStatus); err != nil {
					return fmt.Errorf("dbUpdateServices can't update object log %s %s: %w", objectName, objectID, err)
				}
				slog.Debug(fmt.Sprintf("update svc %s %s %#v", objectName, objectID, *oStatus))
				if err := d.oDb.updateObjectStatus(d.ctx, objectID, oStatus); err != nil {
					return fmt.Errorf("dbUpdateServices can't update object %s %s: %w", objectName, objectID, err)
				}
				if d.byObjectID[objectID].availStatus != oStatus.availStatus {
					slog.Debug(fmt.Sprintf("dbUpdateServices %s avail status %s -> %s", objectName, d.byObjectID[objectID].availStatus, oStatus.availStatus))
				}
				// refresh local cache
				d.byObjectID[objectID].availStatus = oStatus.availStatus
				d.byObjectID[objectID].overallStatus = oStatus.overallStatus
				d.byObjectID[objectID].placement = oStatus.placement
				d.byObjectID[objectID].provisioned = oStatus.provisioned
			}
		}
	}
	return nil
}

func (d *daemonStatus) dbUpdateInstance() error {
	defer logDuration("dbUpdateInstance", time.Now())
	for objectName, obj := range d.byObjectName {
		objID := obj.svcID
		instanceMonitorStates := make(map[string]bool)
		for nodeID, node := range d.byNodeID {
			if node == nil {
				return fmt.Errorf("dbUpdateInstance unexpected nil value for byNodeID(%s)", nodeID)
			}
			nodename := node.nodename
			iStatus := d.data.InstanceStatus(objectName, nodename)
			if iStatus == nil {
				continue
			}
			_, isChanged := d.changes[objectName+"@"+nodename]
			if !isChanged && obj.availStatus != "undef" {
				slog.Debug(fmt.Sprintf("ping instance %s@%s", objectName, nodename))
				changes, err := d.oDb.pingInstance(d.ctx, objID, nodeID)
				if err != nil {
					return fmt.Errorf("dbUpdateInstance can't ping instance %s@%s: %w", objectName, nodename, err)
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
					slog.Debug(fmt.Sprintf("dbUpdateInstance skip for %s@%s subNodeID:%s vs nodeID: %subNodeID", objectName, nodename, subNodeID, nodeID))
					continue
				}
				if iStatus.resources == nil {
					// scaler or wrapper, for example
					if err := d.oDb.instanceStatusDelete(d.ctx, objID, nodeID); err != nil {
						return fmt.Errorf("dbUpdateInstance delete status %s@%s: %w", objID, nodeID, err)
					}
					if err := d.oDb.instanceResourcesDelete(d.ctx, objID, nodeID); err != nil {
						return fmt.Errorf("dbUpdateInstance delete resources %s@%s: %w", objID, nodeID, err)
					}
				} else {
					// set iStatus svcID and nodeID for db update
					iStatus.svcID = objID
					iStatus.nodeID = nodeID
					if err := d.instanceStatusUpdate(objectName, nodename, iStatus); err != nil {
						return fmt.Errorf("dbUpdateInstance update status %s@%s (%s@%s): %w", objectName, nodename, objID, nodeID, err)
					}
					resourceObsoleteAt := time.Now()
					if err := d.instanceResourceUpdate(objectName, nodename, iStatus); err != nil {
						return fmt.Errorf("dbUpdateInstance update resource %s@%s (%s@%s): %w", objectName, nodename, objID, nodeID, err)
					}
					slog.Debug(fmt.Sprintf("dbUpdateInstance deleting obsolete resources %s@%s", objectName, nodename))
					if err := d.oDb.instanceResourcesDeleteObsolete(d.ctx, objID, nodeID, resourceObsoleteAt); err != nil {
						return fmt.Errorf("dbUpdateInstance delete obsolete resources %s@%s: %w", objID, nodeID, err)
					}
				}
			} else {
				if iStatus.resources == nil {
					// scaler or wrapper, for example
					if err := d.oDb.instanceStatusDelete(d.ctx, objID, nodeID); err != nil {
						return fmt.Errorf("dbUpdateInstance delete status %s@%s: %w", objID, nodeID, err)
					}
					if err := d.oDb.instanceResourcesDelete(d.ctx, objID, nodeID); err != nil {
						return fmt.Errorf("dbUpdateInstance delete resources %s@%s: %w", objID, nodeID, err)
					}
				} else {
					resourceObsoleteAt := time.Now()
					for _, containerStatus := range iStatus.Containers() {
						// TODO: update_container_node_fields
						if err := d.instanceStatusUpdate(objID, nodeID, containerStatus); err != nil {
							return fmt.Errorf("dbUpdateInstance update container %s %s@%s (%s@%s): %w",
								containerStatus.monVmName, objID, nodeID, objectName, nodename, err)
						}
					}
					slog.Debug(fmt.Sprintf("dbUpdateInstance deleting obsolete container resources %s@%s", objectName, nodename))
					if err := d.oDb.instanceResourcesDeleteObsolete(d.ctx, objID, nodeID, resourceObsoleteAt); err != nil {
						return fmt.Errorf("dbUpdateInstance delete obsolete container resources %s@%s: %w", objID, nodeID, err)
					}
				}
				// TODO:   update_container_node_fields
			}
			if err := d.oDb.dashboardInstanceFrozenUpdate(d.ctx, objID, nodeID, obj.env, iStatus.monFrozen > 0); err != nil {
				return fmt.Errorf("dbUpdateInstance update dashboard instance frozen %s@%s (%s@%s): %w", objectName, nodename, objID, nodeID, err)
			}
			if err := d.oDb.dashboardDeleteInstanceNotUpdated(d.ctx, objID, nodeID); err != nil {
				return fmt.Errorf("dbUpdateInstance update dashboard instance not updated %s@%s (%s@%s): %w", objectName, nodename, objID, nodeID, err)
			}
			// TODO: verify if we need a placement non optimal alert for object/instance
			//     om2 has: monitor.services.'<path>'.placement = non-optimal
			//     om3 has: cluster.object.<path>.placement_state = non-optimal
			//				cluster.node.<node>.instance.<path>.monitor.is_ha_leader
			//				cluster.node.<node>.instance.<path>.monitor.is_leader
			//     collector v2 calls update_dash_service_not_on_primary (broken since no DEFAULT.autostart_node values)
		}
		if len(instanceMonitorStates) == 1 && instanceMonitorStates["idle"] {
			var remove bool

			remove = slices.Contains([]string{"up", "n/a"}, obj.availStatus)
			if err := d.updateDashboardObject(obj, remove, NewDashboardObjectUnavailable); err != nil {
				return fmt.Errorf("dbUpdateInstance on %s (%s): %w", objID, objectName, err)
			}

			remove = slices.Contains([]string{"optimal", "n/a"}, obj.placement)
			if err := d.updateDashboardObject(obj, remove, NewDashboardObjectPlacement); err != nil {
				return fmt.Errorf("dbUpdateInstance on %s (%s): %w", objID, objectName, err)
			}

			remove = obj.availStatus == "up" && !slices.Contains([]string{"up", "n/a"}, obj.overallStatus)
			if err := d.updateDashboardObject(obj, remove, NewDashboardObjectDegraded); err != nil {
				return fmt.Errorf("dbUpdateInstance on %s (%s): %w", objID, objectName, err)
			}
			// TODO: update_dash_flex_instances_started
			// TODO: update_dash_flex_cpu
		}
	}

	return nil
}

func (d *daemonStatus) instanceResourceUpdate(objName string, nodename string, iStatus *instanceStatus) error {
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

func (d *daemonStatus) instanceStatusUpdate(objName string, nodename string, iStatus *instanceStatus) error {
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

func (d *daemonStatus) dbPurgeInstance() error {
	defer logDuration("dbPurgeInstance", time.Now())
	var nodeIDs, objectNames []string
	for objectName := range d.byObjectName {
		objectNames = append(objectNames, objectName)
	}
	for nodeID := range d.byNodeID {
		nodeIDs = append(nodeIDs, nodeID)
	}
	instanceIDs, err := d.oDb.getOrphanInstances(d.ctx, nodeIDs, objectNames)
	if err != nil {
		return fmt.Errorf("dbPurgeInstance: getOrphanInstances: %w", err)
	}
	for _, instanceID := range instanceIDs {
		if err1 := d.oDb.purgeInstances(d.ctx, instanceID); err1 != nil {
			err = errors.Join(err, fmt.Errorf("purge instance %v: %w", instanceID, err1))
		}
	}
	if err != nil {
		return fmt.Errorf("dbPurgeInstance: %w", err)
	}
	return nil
}

func (d *daemonStatus) dbPurgeService() error {
	defer logDuration("dbPurgeService", time.Now())
	objectIDs, err := d.oDb.objectIDWithPurgeTag(d.ctx, d.clusterID)
	if err != nil {
		err = fmt.Errorf("dbPurgeService: objectIDWithPurgeTag: %w", err)
		return err
	}
	for _, objectID := range objectIDs {
		if err1 := d.oDb.purgeObject(d.ctx, objectID); err1 != nil {
			err = errors.Join(err, fmt.Errorf("purge object %s: %w", objectID, err1))
		}
	}
	if err != nil {
		return fmt.Errorf("dbPurgeService: %w", err)
	}
	return nil
}

func (d *daemonStatus) pushFromTableChanges() error {
	defer logDuration("pushFromTableChanges", time.Now())
	for _, tableName := range d.oDb.tableChanges() {
		slog.Debug(fmt.Sprintf("pushFromTableChanges %s", tableName))
		if err := d.oDb.updateTableModified(d.ctx, tableName); err != nil {
			return fmt.Errorf("pushFromTableChanges: %w", err)
		}
		if err := d.ev.EventPublish(tableName+"_change", nil); err != nil {
			return fmt.Errorf("EventPublish send %s: %w", tableName, err)
		}
	}
	return nil
}

func logDuration(s string, begin time.Time) {
	slog.Debug(fmt.Sprintf("STAT: %s elapse: %s", s, time.Now().Sub(begin)))
}

func logDurationInfo(s string, begin time.Time) {
	slog.Info(fmt.Sprintf("STAT: %s elapse: %s", s, time.Now().Sub(begin)))
}
