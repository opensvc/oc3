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
		app       string
		nodeEnv   string
	}

	DBObject struct {
		svcname   string
		svcID     string
		clusterID string
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
	}

	daemonStatus struct {
		ctx   context.Context
		redis *redis.Client
		db    *sql.DB
		oDb   *opensvcDB

		nodeID      string
		clusterID   string
		clusterName string
		nodeApp     string
		nodeEnv     string
		callerNode  *DBNode

		changes []string
		rawData []byte

		data dataProvider

		byNodename map[string]*DBNode
		byNodeID   map[string]*DBNode

		byObjectName map[string]*DBObject
		byObjectID   map[string]*DBObject

		tableChange map[string]struct{}

		byInstanceName map[string]*DBInstance
		byInstanceID   map[string]*DBInstance
	}
)

func (t *Worker) handleDaemonStatus(nodeID string) error {
	d := daemonStatus{
		ctx:    context.Background(),
		redis:  t.Redis,
		db:     t.DB,
		nodeID: nodeID,
		oDb:    &opensvcDB{db: t.DB},

		byNodename: make(map[string]*DBNode),
		byNodeID:   make(map[string]*DBNode),

		byObjectID:   make(map[string]*DBObject),
		byObjectName: make(map[string]*DBObject),

		byInstanceID:   make(map[string]*DBInstance),
		byInstanceName: make(map[string]*DBInstance),

		tableChange: make(map[string]struct{}),
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
	)
	if err != nil {
		return fmt.Errorf("handleDaemonStatus node_id %s cluster_id %s: %w", nodeID, d.clusterID, err)
	}

	slog.Info(fmt.Sprintf("handleDaemonStatus done: node_id: %s cluster_id: %s, cluster_name: %s changes: %s, byNodes: %#v",
		d.nodeID, d.clusterID, d.clusterName, d.changes, d.byNodename))
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
	if err := d.redis.HDel(d.ctx, cache.KeyDaemonStatusPending, d.nodeID).Err(); err != nil {
		return fmt.Errorf("dropPening: HDEL %s %s: %w", cache.KeyDaemonStatusPending, d.nodeID, err)
	}
	return nil
}

func (d *daemonStatus) getChanges() error {
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
	} else {
		return fmt.Errorf("getChanges: HGET %s %s: %w", cache.KeyDaemonStatusChangesHash, d.nodeID, err)
	}
	d.changes = strings.Fields(s)
	return nil
}

func (d *daemonStatus) getData() error {
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
	slog.Info(fmt.Sprintf("dbCheckClusters INSERT or UPDATE clusters for node_id %s with cluster_id %s", d.nodeID, d.clusterID))
	if _, err := d.db.ExecContext(d.ctx, query, d.clusterName, clusterData, d.clusterID, d.clusterName, clusterData); err != nil {
		return fmt.Errorf("dbCheckClusters can't update clusters nodeID: %s clusterID: %s: %w", d.nodeID, d.clusterID, err)
	}
	return nil
}

func (d *daemonStatus) dbFindNodes() error {
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
	return nil
}

func (d *daemonStatus) dataToNodeFrozen() error {
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
			d.addTableChange("nodes")
		}
	}
	return nil
}

func (d *daemonStatus) addTableChange(s ...string) {
	for _, table := range s {
		d.tableChange[table] = struct{}{}
	}
}

func (d *daemonStatus) dbFindServices() error {
	const queryFindServicesInfo = "SELECT svcname, svc_id, cluster_id" +
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
		if err := rows.Scan(&o.svcname, &o.svcID, &o.clusterID); err != nil {
			return fmt.Errorf("dbFindServices scan %s: %w", d.nodeID, err)
		}
		d.byObjectName[o.svcname] = &o
		d.byObjectID[o.svcID] = &o
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("dbFindServices FindClusterNodesInfo %s: %w", d.nodeID, err)
	}
	return nil
}

func (d *daemonStatus) dbFindInstance() error {
	const querySelect = "" +
		"SELECT svc_id, node_id, mon_frozen" +
		" FROM svcmon" +
		" WHERE svc_id IN (?"

	values := []any{}
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
			return fmt.Errorf("dbFindServices scan %s: %w", d.nodeID, err)
		}
		if n, ok := d.byNodeID[o.nodeID]; ok {
			// Only pickup instances from known nodes
			if s, ok := d.byObjectID[o.svcID]; ok {
				// Only pickup instances from known objects
				d.byInstanceName[s.svcname+"@"+n.nodename] = &o
				d.byInstanceID[s.svcID+"@"+n.nodeID] = &o
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
	nodes := make([]string, 0)
	for nodename := range d.byNodename {
		nodes = append(nodes, nodename)
	}
	for _, objectName := range missing {
		app := d.data.appFromObjectName(objectName, nodes...)
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
