package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type (
	ResourceMeta struct {
		ID  int64
		OID uuid.UUID
		NID uuid.UUID
		RID string
	}
)

func (t ResourceMeta) String() string {
	return fmt.Sprintf("%s@%s:%s", t.OID, t.NID, t.RID)
}

func (oDb *DB) ResourceOutdatedLists(ctx context.Context) (resources []ResourceMeta, err error) {
	sql := `SELECT id, rid, svc_id, node_id FROM resmon
                WHERE updated < DATE_SUB(NOW(), INTERVAL 15 MINUTE)
                AND res_status != "undef"`
	rows, err := oDb.DB.QueryContext(ctx, sql)
	if err != nil {
		return
	}
	defer rows.Close()

	for {
		next := rows.Next()
		if !next {
			break
		}
		var resource ResourceMeta
		rows.Scan(&resource.ID, &resource.RID, &resource.OID, &resource.NID)
		resources = append(resources, resource)
	}
	return
}

// ResourceUpdateLog handle resmon_log_last and resmon_log avail value changes.
//
// resmon_log_last tracks the current avail value from begin to now.
// resmon_log tracks avail values changes with begin and end: [(avail, begin, end), ...]
func (oDb *DB) ResourceUpdateLog(ctx context.Context, resource ResourceMeta, avail string) error {
	defer logDuration("ResourceUpdateLog "+resource.String(), time.Now())
	const (
		qGetLogLast = "SELECT `res_status`, `res_begin` FROM `resmon_log_last` WHERE `svc_id` = ? AND `node_id` = ? AND `rid` = ?"
		qSetLogLast = "" +
			"INSERT INTO `resmon_log_last` (`svc_id`, `node_id`, `rid`, `res_begin`, `res_end`, `res_status`)" +
			" VALUES (?, ?, ?, NOW(), NOW(), ?)" +
			" ON DUPLICATE KEY UPDATE `res_begin` = NOW(), `res_end` = NOW(), `res_status` = ?"
		qExtendIntervalOfCurrentAvail                = "UPDATE `resmon_log_last` SET `res_end` = NOW() WHERE `svc_id` = ? AND `node_id` = ? AND `rid` = ?"
		qSaveIntervalOfPreviousAvailBeforeTransition = "" +
			"INSERT INTO `resmon_log` (`svc_id`, `node_id`, `rid`, `res_begin`, `res_end`, `res_status`)" +
			" VALUES (?, ?, ?, ?, NOW(), ?)"
	)
	var (
		previousAvail string

		previousBegin time.Time
	)
	setLogLast := func() error {
		_, err := oDb.DB.ExecContext(ctx, qSetLogLast, resource.OID, resource.NID, resource.RID, avail, avail)
		if err != nil {
			return fmt.Errorf("objectUpdateLog can't update resmon_log_last %s: %w", resource, err)
		}
		return nil
	}
	err := oDb.DB.QueryRowContext(ctx, qGetLogLast, resource.OID, resource.NID, resource.RID).Scan(&previousAvail, &previousBegin)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// set initial avail value
		defer oDb.SetChange("resmon_log")
		return setLogLast()
	case err != nil:
		return fmt.Errorf("objectUpdateLog can't get resmon_log_last %s: %w", resource, err)
	default:
		defer oDb.SetChange("resmon_log")
		if previousAvail == avail {
			// no change, extend last interval
			if _, err := oDb.DB.ExecContext(ctx, qExtendIntervalOfCurrentAvail, resource.OID, resource.NID, resource.RID); err != nil {
				return fmt.Errorf("objectUpdateLog can't set resmon_log_last.res_end %s: %w", resource, err)
			}
			return nil
		} else {
			// the avail value will change, save interval of previous avail value before change
			if _, err := oDb.DB.ExecContext(ctx, qSaveIntervalOfPreviousAvailBeforeTransition, resource.OID, resource.NID, resource.RID, previousBegin, previousAvail); err != nil {
				return fmt.Errorf("objectUpdateLog can't save resmon_log change %s: %w", resource, err)
			}
			// reset begin and end interval for new avail
			return setLogLast()
		}
	}
}

func (oDb *DB) ResourceUpdateStatus(ctx context.Context, resources []ResourceMeta, status string) (n int64, err error) {
	idsLen := len(resources)
	sql := `UPDATE resmon
                SET res_status=?, updated=NOW()
                WHERE id IN (%s)`
	sql = fmt.Sprintf(sql, Placeholders(idsLen))

	args := make([]any, idsLen+1)
	args[0] = status
	for i, resource := range resources {
		args[i+1] = resource.ID
	}

	if n, err = oDb.execCountContext(ctx, sql, args...); err != nil {
		return 0, err
	} else if n > 0 {
		oDb.Session.SetChanges("resmon")
	}
	return n, err
}

func (oDb *DB) PurgeResmonOutdated(ctx context.Context) error {
	var query = `DELETE
		FROM resmon
		WHERE
		  updated < DATE_SUB(NOW(), INTERVAL 1 DAY)`
	if count, err := oDb.execCountContext(ctx, query); err != nil {
		return err
	} else if count > 0 {
		oDb.SetChange("resmon")
	}
	return nil
}
