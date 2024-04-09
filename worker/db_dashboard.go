package worker

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type (
	Dashboard struct {
		ID       int64
		ObjectID string
		NodeID   string
		Type     string
		Fmt      string
		Dict     string
		Severity int
		Env      string
		Instance string
		Created  time.Time
		Updated  time.Time
	}
)

// dashboardInstanceFrozenUpdate update or remove the "service frozen" alerts for instance
func (oDb *opensvcDB) dashboardInstanceFrozenUpdate(ctx context.Context, objectID, nodeID string, objectEnv string, frozen bool) error {
	defer logDuration("dashboardInstanceFrozenUpdate", time.Now())
	const (
		queryThawed = `
			DELETE FROM dashboard
			WHERE 
			    dash_type = 'service frozen' 
			  AND svc_id = ?
			  AND node_id = ?
			`
		queryFrozen = `
			INSERT INTO dashboard
			SET
              dash_type = 'service frozen', svc_id = ?, node_id = ?,
              dash_severity = 1, dash_fmt='', dash_dict='',
			  dash_created = NOW(), dash_updated = NOW(), dash_env = ?
		    ON DUPLICATE KEY UPDATE
			  dash_severity = 1, dash_fmt = '', dash_dict = '',
			  dash_updated = NOW(), dash_env = ?`
	)
	var (
		err    error
		result sql.Result
	)
	switch frozen {
	case true:
		result, err = oDb.db.ExecContext(ctx, queryFrozen, objectID, nodeID, objectEnv, objectEnv)
		if err != nil {
			return fmt.Errorf("update dashboard 'service frozen' for %s@%s: %w", objectID, nodeID, err)
		}
	case false:
		result, err = oDb.db.ExecContext(ctx, queryThawed, objectID, nodeID)
		if err != nil {
			return fmt.Errorf("delete dashboard 'service frozen' for %s@%s: %w", objectID, nodeID, err)
		}
	}
	if count, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("count dashboard 'service frozen' for %s@%s: %w", objectID, nodeID, err)
	} else if count > 0 {
		oDb.tableChange("dashboard")
	}
	return nil
}

// dashboardDeleteInstanceNotUpdated delete "instance status not updated" alerts.
func (oDb *opensvcDB) dashboardDeleteInstanceNotUpdated(ctx context.Context, objectID, nodeID string) error {
	defer logDuration("dashboardDeleteInstanceNotUpdated", time.Now())
	const (
		query = `DELETE FROM dashboard WHERE svc_id = ? AND node_id = ? AND dash_type = 'instance status not updated'`
	)
	if result, err := oDb.db.ExecContext(ctx, query, objectID, nodeID); err != nil {
		return err
	} else if count, err := result.RowsAffected(); err != nil {
		return err
	} else if count > 0 {
		oDb.tableChange("dashboard")
	}
	return nil
}

// dashboardDeleteObjectWithType delete from dashboard where svc_id and dash_type match
func (oDb *opensvcDB) dashboardDeleteObjectWithType(ctx context.Context, objectID, dashType string) error {
	defer logDuration("dashboardDeleteObjectWithType: "+dashType, time.Now())
	const (
		query = `DELETE FROM dashboard WHERE svc_id = ? AND dash_type = '?'`
	)
	if result, err := oDb.db.ExecContext(ctx, query, objectID, dashType); err != nil {
		return fmt.Errorf("dashboardDeleteObjectWithType %s: %w", dashType, err)
	} else if count, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("dashboardDeleteObjectWithType %s: %w", dashType, err)
	} else if count > 0 {
		oDb.tableChange("dashboard")
	}
	return nil
}

// ObjectInAckUnavailabilityPeriod returns true if objectID is in acknowledge unavailability period.
func (oDb *opensvcDB) ObjectInAckUnavailabilityPeriod(ctx context.Context, objectID string) (ok bool, err error) {
	defer logDuration("ObjectInAckUnavailabilityPeriod", time.Now())
	const (
		query = `SELECT COUNT(*) FROM svcmon_log_ack WHERE svc_id = ? AND mon_begin <= NOW() AND mon_end >= NOW()`
	)
	var count uint64
	err = oDb.db.QueryRowContext(ctx, query, objectID).Scan(&count)
	if err != nil {
		err = fmt.Errorf("ObjectInAckUnavailabilityPeriod: %w", err)
	}
	return count > 0, err
}

// dashboardUpdateObject delete "service unavailable" alerts.
func (oDb *opensvcDB) dashboardUpdateObject(ctx context.Context, d *Dashboard) error {
	defer logDuration("dashboardUpdateObject", time.Now())
	const (
		query = `INSERT INTO dashboard
        	SET
				svc_id = ?,
				dash_type = ?,
				dash_fmt = ?,
				dash_severity = ?,
				dash_dict = ?,
				dash_created = NOW(),
				dash_updated = NOW(),
				dash_env = ?
			ON DUPLICATE KEY UPDATE
				dash_fmt = ?,
				dash_severity = ?,
				dash_dict = ?,
				dash_updated = NOW(),
				dash_env = ?
				`
	)
	result, err := oDb.db.ExecContext(ctx, query,
		d.ObjectID, d.Type, d.Fmt, d.Severity, d.Dict, d.Env,
		d.Fmt, d.Severity, d.Dict, d.Env)
	if err != nil {
		return fmt.Errorf("dashboardUpdateObject: %w", err)
	} else if count, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("dashboardUpdateObject: %w", err)
	} else if count > 0 {
		oDb.tableChange("dashboard")
	}
	return nil
}
