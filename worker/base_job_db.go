package worker

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/opensvc/oc3/cdb"
)

type (
	JobDB struct {
		dbPool *sql.DB

		// db is the generic DB operator
		db cdb.DBOperater

		// oDb is the DB collector helper
		oDb *cdb.DB

		now time.Time
	}
)

var (
	_ cdb.DBTxer = &JobDB{}
)

func (j *JobDB) PrepareDB(ctx context.Context, dbPool *sql.DB, ev EventPublisher, withTx bool) error {
	if j == nil {
		return fmt.Errorf("nil JobDB")
	}
	odb := cdb.New(dbPool)
	if withTx {
		if err := odb.CreateTx(ctx, nil); err != nil {
			return err
		}
	}
	odb.CreateSession(ev)

	j.oDb = odb

	j.db = odb.DB
	return nil
}

func (j *JobDB) CreateTx(ctx context.Context) error {
	return j.oDb.CreateTx(ctx, nil)
}

func (j *JobDB) CommitAndCreateTx(ctx context.Context) error {
	if err := j.oDb.Commit(); err != nil {
		return fmt.Errorf("CommitAndCreateTx pre-commit: %w", err)
	}
	if err := j.oDb.CreateTx(ctx, nil); err != nil {
		return fmt.Errorf("CommitAndCreateTx: %w", err)
	}
	return nil
}

func (j *JobDB) Commit() error {
	return j.oDb.Commit()
}

func (j *JobDB) Rollback() error {
	return j.oDb.Rollback()
}

func (j *JobDB) DB() cdb.DBOperater {
	return j.db
}

func (j *JobDB) dbNow(ctx context.Context) (err error) {
	rows, err := j.db.QueryContext(ctx, "SELECT NOW()")
	if err != nil {
		return err
	}
	if rows == nil {
		return fmt.Errorf("no result rows for SELECT NOW()")
	}
	defer rows.Close()
	if !rows.Next() {
		return fmt.Errorf("no result rows next for SELECT NOW()")
	}
	if err := rows.Scan(&j.now); err != nil {
		return err
	}
	return nil
}

func (d *JobDB) dbUpdateInstance(ctx context.Context, iStatus *instanceData, objID string, nodeID string, objectName string, nodename string, obj *cdb.DBObject, instanceMonitorStates map[string]bool, node *cdb.DBNode, beginInstance time.Time, changes map[string]struct{}) error {
	iStatus.SvcID = objID
	iStatus.NodeID = nodeID
	_, isChanged := changes[objectName+"@"+nodename]
	if !isChanged && obj.AvailStatus != "undef" {
		slog.Debug(fmt.Sprintf("ping instance %s@%s", objectName, nodename))
		changes, err := d.oDb.InstancePing(ctx, objID, nodeID)
		if err != nil {
			return fmt.Errorf("dbUpdateInstances can't ping instance %s@%s: %w", objectName, nodename, err)
		} else if changes {
			// the instance already existed, and the updated tstamp has been refreshed
			// skip the inserts/updates
			return nil
		}
	}
	instanceMonitorStates[iStatus.MonSmonStatus] = true
	if iStatus.encap == nil {
		subNodeID, _, _, err := d.oDb.TranslateEncapNodename(ctx, objID, nodeID)
		if err != nil {
			return err
		}
		if subNodeID != "" && subNodeID != nodeID {
			slog.Debug(fmt.Sprintf("dbUpdateInstances skip for %s@%s subNodeID:%s vs nodeID: %subNodeID", objectName, nodename, subNodeID, nodeID))
			return nil
		}
		if iStatus.resources == nil {
			// scaler or wrapper, for example
			if err := d.oDb.InstanceDeleteStatus(ctx, objID, nodeID); err != nil {
				return fmt.Errorf("dbUpdateInstances delete status %s@%s: %w", objID, nodeID, err)
			}
			if err := d.oDb.InstanceResourcesDelete(ctx, objID, nodeID); err != nil {
				return fmt.Errorf("dbUpdateInstances delete resources %s@%s: %w", objID, nodeID, err)
			}
		} else {
			if err := d.instanceStatusUpdate(ctx, objectName, nodename, iStatus); err != nil {
				return fmt.Errorf("dbUpdateInstances update status %s@%s (%s@%s): %w", objectName, nodename, objID, nodeID, err)
			}
			if err := d.instanceResourceUpdate(ctx, objectName, nodename, iStatus); err != nil {
				return fmt.Errorf("dbUpdateInstances update resource %s@%s (%s@%s): %w", objectName, nodename, objID, nodeID, err)
			}
			slog.Debug(fmt.Sprintf("dbUpdateInstances deleting obsolete resources %s@%s", objectName, nodename))
			if err := d.oDb.InstanceResourcesDeleteObsolete(ctx, objID, nodeID, d.now); err != nil {
				return fmt.Errorf("dbUpdateInstances delete obsolete resources %s@%s: %w", objID, nodeID, err)
			}
		}
	} else {
		if iStatus.resources == nil {
			// scaler or wrapper, for example
			if err := d.oDb.InstanceDeleteStatus(ctx, objID, nodeID); err != nil {
				return fmt.Errorf("dbUpdateInstances delete status %s@%s: %w", objID, nodeID, err)
			}
			if err := d.oDb.InstanceResourcesDelete(ctx, objID, nodeID); err != nil {
				return fmt.Errorf("dbUpdateInstances delete resources %s@%s: %w", objID, nodeID, err)
			}
		} else {
			for _, containerStatus := range iStatus.Containers() {
				slog.Debug(fmt.Sprintf("dbUpdateInstances from container status %s@%s monVmName: %s monVmType: %s", objectName, nodename, containerStatus.MonVmName, containerStatus.MonVmType))
				if containerStatus == nil {
					continue
				}
				if containerStatus.fromOutsideStatus == "up" {
					slog.Debug(fmt.Sprintf("dbUpdateInstances nodeContainerUpdateFromParentNode %s@%s encap hostname %s",
						objID, nodeID, containerStatus.MonVmName))
					if err := d.oDb.NodeContainerUpdateFromParentNode(ctx, containerStatus.MonVmName, obj.App, node); err != nil {
						return fmt.Errorf("dbUpdateInstances nodeContainerUpdateFromParentNode %s@%s encap hostname %s: %w",
							objID, nodeID, containerStatus.MonVmName, err)
					}
				}

				if err := d.instanceStatusUpdate(ctx, objID, nodeID, containerStatus); err != nil {
					return fmt.Errorf("dbUpdateInstances update container %s %s@%s (%s@%s): %w",
						containerStatus.MonVmName, objID, nodeID, objectName, nodename, err)
				}
				if err := d.instanceResourceUpdate(ctx, objectName, nodename, iStatus); err != nil {
					return fmt.Errorf("dbUpdateInstances update resource %s@%s (%s@%s): %w", objectName, nodename, objID, nodeID, err)
				}
			}
			slog.Debug(fmt.Sprintf("dbUpdateInstances deleting obsolete container resources %s@%s", objectName, nodename))
			if err := d.oDb.InstanceResourcesDeleteObsolete(ctx, objID, nodeID, d.now); err != nil {
				return fmt.Errorf("dbUpdateInstances delete obsolete container resources %s@%s: %w", objID, nodeID, err)
			}
		}
	}
	if err := d.oDb.DashboardInstanceFrozenUpdate(ctx, objID, nodeID, obj.Env, iStatus.MonFrozen > 0); err != nil {
		return fmt.Errorf("dbUpdateInstances update dashboard instance frozen %s@%s (%s@%s): %w", objectName, nodename, objID, nodeID, err)
	}
	if err := d.oDb.DashboardDeleteInstanceNotUpdated(ctx, objID, nodeID); err != nil {
		return fmt.Errorf("dbUpdateInstances update dashboard instance not updated %s@%s (%s@%s): %w", objectName, nodename, objID, nodeID, err)
	}
	// TODO: verify if we need a placement non optimal alert for object/instance
	//     om2 has: monitor.services.'<path>'.placement = non-optimal
	//     om3 has: cluster.object.<path>.placement_state = non-optimal
	//				cluster.node.<node>.instance.<path>.monitor.is_ha_leader
	//				cluster.node.<node>.instance.<path>.monitor.is_leader
	//     collector v2 calls update_dash_service_not_on_primary (broken since no DEFAULT.autostart_node values)

	slog.Debug(fmt.Sprintf("STAT: dbUpdateInstances instance duration %s@%s %s", objectName, nodename, time.Since(beginInstance)))
	return nil
}

func (d *JobDB) pushFromTableChanges(ctx context.Context) error {
	return d.oDb.Session.NotifyChanges(ctx)
}

func (d *JobDB) instanceStatusUpdate(ctx context.Context, objName string, nodename string, iStatus *instanceData) error {
	slog.Debug(fmt.Sprintf("updating instance status %s@%s (%s@%s)", objName, nodename, iStatus.SvcID, iStatus.NodeID))
	if err := d.oDb.InstanceStatusUpdate(ctx, &iStatus.DBInstanceStatus); err != nil {
		return fmt.Errorf("update instance status: %w", err)
	}
	slog.Debug(fmt.Sprintf("instanceStatusUpdate updating status log %s@%s (%s@%s)", objName, nodename, iStatus.SvcID, iStatus.NodeID))
	err := d.oDb.InstanceStatusLogUpdate(ctx, &iStatus.DBInstanceStatus)
	if err != nil {
		return fmt.Errorf("update instance status log: %w", err)
	}
	return nil
}

func (d *JobDB) instanceResourceUpdate(ctx context.Context, objName string, nodename string, iStatus *instanceData) error {
	for _, res := range iStatus.InstanceResources() {
		slog.Debug(fmt.Sprintf("updating instance resource %s@%s %s (%s@%s)", objName, nodename, res.RID, iStatus.SvcID, iStatus.NodeID))
		if err := d.oDb.InstanceResourceUpdate(ctx, res); err != nil {
			return fmt.Errorf("update resource %s: %w", res.RID, err)
		}
		slog.Debug(fmt.Sprintf("updating instance resource log %s@%s %s (%s@%s)", objName, nodename, res.RID, iStatus.SvcID, iStatus.NodeID))
		if err := d.oDb.InstanceResourceLogUpdate(ctx, res); err != nil {
			return fmt.Errorf("update resource log %s: %w", res.RID, err)
		}
	}
	return nil
}
