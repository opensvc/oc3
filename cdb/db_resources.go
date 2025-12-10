package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

func (oDb *DB) ResourceOutdatedLists(ctx context.Context) (ids []int64, rids []string, svcIDs []uuid.UUID, nodeIDs []uuid.UUID, err error) {
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
		var svcID uuid.UUID
		var nodeID uuid.UUID
		var rid string
		var id int64
		rows.Scan(&id, &rid, &svcID, &nodeID)
		svcIDs = append(svcIDs, svcID)
		nodeIDs = append(nodeIDs, nodeID)
		rids = append(rids, rid)
	}
	return
}

// ResourceUpdateLog handle resmon_log_last and resmon_log avail value changes.
//
// resmon_log_last tracks the current avail value from begin to now.
// resmon_log tracks avail values changes with begin and end: [(avail, begin, end), ...]
func (oDb *DB) ResourceUpdateLog(ctx context.Context, rid string, svcID, nodeID uuid.UUID, avail string) error {
	name := fmt.Sprintf("%s@%s:%s", svcID, nodeID, rid)
	defer logDuration("ResourceUpdateLog "+name, time.Now())
	const (
		qGetLogLast = "SELECT `res_status`, `res_begin` FROM `resmon_log_last` WHERE `res_id` = ?"
		qSetLogLast = "" +
			"INSERT INTO `resmon_log_last` (`svc_id`, `res_begin`, `res_end`, `res_status`)" +
			" VALUES (?, NOW(), NOW(), ?)" +
			" ON DUPLICATE KEY UPDATE `res_begin` = NOW(), `res_end` = NOW(), `res_status` = ?"
		qExtendIntervalOfCurrentAvail                = "UPDATE `resmon_log_last` SET `res_end` = NOW() WHERE `svc_id` = ?"
		qSaveIntervalOfPreviousAvailBeforeTransition = "" +
			"INSERT INTO `resmon_log` (`svc_id`, `res_begin`, `res_end`, `res_status`)" +
			" VALUES (?, ?, NOW(), ?)"
	)
	var (
		previousAvail string

		previousBegin time.Time
	)
	setLogLast := func() error {
		_, err := oDb.DB.ExecContext(ctx, qSetLogLast, svcID, avail, avail)
		if err != nil {
			return fmt.Errorf("objectUpdateLog can't update resmon_log_last %s: %w", svcID, err)
		}
		return nil
	}
	err := oDb.DB.QueryRowContext(ctx, qGetLogLast, svcID).Scan(&previousAvail, &previousBegin)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// set initial avail value
		defer oDb.SetChange("resmon_log")
		return setLogLast()
	case err != nil:
		return fmt.Errorf("objectUpdateLog can't get resmon_log_last %s: %w", svcID, err)
	default:
		defer oDb.SetChange("resmon_log")
		if previousAvail == avail {
			// no change, extend last interval
			if _, err := oDb.DB.ExecContext(ctx, qExtendIntervalOfCurrentAvail, svcID); err != nil {
				return fmt.Errorf("objectUpdateLog can't set resmon_log_last.res_end %s: %w", svcID, err)
			}
			return nil
		} else {
			// the avail value will change, save interval of previous avail value before change
			if _, err := oDb.DB.ExecContext(ctx, qSaveIntervalOfPreviousAvailBeforeTransition, svcID, previousBegin, previousAvail); err != nil {
				return fmt.Errorf("objectUpdateLog can't save resmon_log change %s: %w", svcID, err)
			}
			// reset begin and end interval for new avail
			return setLogLast()
		}
	}
}

func (oDb *DB) ResourceUpdateStatus(ctx context.Context, ids []int64, status string) (n int64, err error) {
	idsLen := len(ids)
	sql := `UPDATE resmon
                SET res_status=?, updated=NOW()
                WHERE id IN (%s)`
	sql = fmt.Sprintf(sql, Placeholders(idsLen))

	args := make([]any, idsLen+1)
	args[0] = status
	for i, id := range ids {
		args[i+1] = id
	}

	result, err := oDb.DB.ExecContext(ctx, sql, args...)
	if err != nil {
		return 0, err
	}
	n, err = result.RowsAffected()
	if err == nil && n > 0 {
		oDb.Session.SetChanges("resmon")
	}
	return n, err
}
