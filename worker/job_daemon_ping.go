package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/opensvc/oc3/cache"
)

type (
	daemonPing struct {
		ctx   context.Context
		redis *redis.Client
		db    DBOperater
		oDb   *opensvcDB
		ev    EventPublisher

		nodeID     string
		clusterID  string
		callerNode *DBNode

		byObjectID map[string]*DBObject
		byNodeID   map[string]*DBNode
	}
)

func (t *Worker) handleDaemonPing(nodeID string) error {
	defer logDurationInfo(fmt.Sprintf("handleDaemonPing %s with tx %v", nodeID, t.WithTx), time.Now())
	slog.Info(fmt.Sprintf("handleDaemonPing starting for node_id %s", nodeID))
	ctx := context.Background()

	d := daemonPing{
		ctx:    ctx,
		redis:  t.Redis,
		nodeID: nodeID,
		ev:     t.Ev,

		byNodeID:   make(map[string]*DBNode),
		byObjectID: make(map[string]*DBObject),
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

	type operation struct {
		desc string
		do   func() error
	}

	chain := func(ops ...operation) error {
		for _, op := range ops {
			begin := time.Now()
			err := op.do()
			duration := time.Now().Sub(begin)
			if err != nil {
				operationDuration.
					With(prometheus.Labels{"desc": op.desc, "status": operationStatusFailed}).
					Observe(duration.Seconds())
				return err
			}
			operationDuration.
				With(prometheus.Labels{"desc": op.desc, "status": operationStatusOk}).
				Observe(duration.Seconds())
			slog.Debug(fmt.Sprintf("STAT: %s elapse: %s", op.desc, duration))
		}
		return nil
	}

	err := chain([]operation{
		{"daemonPing/dropPending", d.dropPending},
		{"daemonPing/dbFetchNodes", d.dbFetchNodes},
		{"daemonPing/dbFetchObjects", d.dbFetchObjects},
		{"daemonPing/dbPingInstances", d.dbPingInstances},
		{"daemonPing/dbPingObjects", d.dbPingObjects},
		{"daemonPing/pushFromTableChanges", d.pushFromTableChanges},
	}...)
	if err != nil {
		if tx, ok := d.db.(DBTxer); ok {
			slog.Debug("handleDaemonPing rollback on error")
			if err := tx.Rollback(); err != nil {
				slog.Error(fmt.Sprintf("handleDaemonPing rollback failed: %s", err))
			}
		}
		return fmt.Errorf("handleDaemonPing node_id %s cluster_id %s: %w", nodeID, d.clusterID, err)
	} else if tx, ok := d.db.(DBTxer); ok {
		slog.Debug("handleDaemonPing commit")
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("handleDaemonPing commit: %w", err)
		}
	}
	slog.Info(fmt.Sprintf("handleDaemonPing done for %s", d.nodeID))
	return nil
}

func (d *daemonPing) dropPending() error {
	if err := d.redis.HDel(d.ctx, cache.KeyDaemonPingPending, d.nodeID).Err(); err != nil {
		return fmt.Errorf("dropPending: HDEL %s %s: %w", cache.KeyDaemonPingPending, d.nodeID, err)
	}
	return nil
}

// dbFetchNodes fetch nodes (that are associated with caller node ID) from database
// and sets d.byNodeID and d.clusterID.
func (d *daemonPing) dbFetchNodes() (err error) {
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

func (d *daemonPing) dbFetchObjects() (err error) {
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
func (d *daemonPing) dbPingInstances() error {
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
func (d *daemonPing) dbPingObjects() (err error) {
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

func (d *daemonPing) pushFromTableChanges() error {
	return pushFromTableChanges(d.ctx, d.oDb, d.ev)
}
