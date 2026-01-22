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
	SvcAction struct {
		ID     int64
		SvcID  uuid.UUID
		NodeID uuid.UUID
	}

	BActionErrorCount struct {
		SvcID    string
		NodeID   string
		ErrCount int
		SvcEnv   *string
	}
)

func (oDb *DB) BActionErrorsRefresh(ctx context.Context) error {
	_, err := oDb.DB.ExecContext(ctx, "TRUNCATE b_action_errors")
	if err != nil {
		return err
	}

	sql := `INSERT INTO b_action_errors (
             SELECT NULL, a.svc_id, a.node_id, count(a.id)
             FROM svcactions a
             WHERE
               a.end>DATE_SUB(NOW(), INTERVAL 1 DAY) AND
               a.status='err' AND
               isnull(a.ack) AND
               a.end IS NOT NULL
             GROUP BY a.svc_id, a.node_id
        )`

	_, err = oDb.DB.ExecContext(ctx, sql)
	if err != nil {
		return err
	}
	return nil
}

func argsFromIDs(ids []int64) []any {
	l := make([]any, len(ids))
	for i, id := range ids {
		l[i] = any(id)
	}
	return l
}

func (oDb *DB) AutoAckActionErrors(ctx context.Context, ids []int64) error {
	request := fmt.Sprintf(`UPDATE svcactions
             SET
               ack=1,
               acked_date=NOW(),
               acked_comment="Automatically acknowledged",
               acked_by="admin@opensvc.com"
	     WHERE
                 id IN (%s)`, Placeholders(len(ids)))
	args := argsFromIDs(ids)
	result, err := oDb.DB.ExecContext(ctx, request, args...)
	if err != nil {
		return err
	}
	if rowAffected, err := result.RowsAffected(); err != nil {
		return err
	} else if rowAffected > 0 {
		oDb.SetChange("svcactions")
	}
	return nil
}

func (oDb *DB) LogActionErrorsNotAcked(ctx context.Context, ids []int64) error {
	request := fmt.Sprintf(`INSERT IGNORE INTO log
               SELECT NULL,
                      "service.action.notacked",
                      "scheduler",
                      "unacknowledged failed action '%%(action)s' at '%%(begin)s'",
                      concat('{"action": "', action, '", "begin": "', begin, '"}'),
                      NOW(),
                      svc_id,
                      0,
                      0,
                      MD5(CONCAT("service.action.notacked",node_id,svc_id,begin)),
                      "warning",
                      node_id
               FROM svcactions
               WHERE
                 id IN (%s)`, Placeholders(len(ids)))
	args := argsFromIDs(ids)
	result, err := oDb.DB.ExecContext(ctx, request, args...)
	if err != nil {
		return err
	}
	if rowAffected, err := result.RowsAffected(); err != nil {
		return err
	} else if rowAffected > 0 {
		oDb.SetChange("log")
	}
	return nil
}

func (oDb *DB) GetActionErrorsNotAcked(ctx context.Context) (ids []int64, err error) {
	var age = 1
	var query = fmt.Sprintf(`SELECT
                 id
               FROM svcactions
	       WHERE
                 status="err" AND
                 ack IS NULL AND
                 begin<DATE_SUB(NOW(), INTERVAL %d DAY)`, age)

	var rows *sql.Rows

	rows, err = oDb.DB.QueryContext(ctx, query)
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var id int64
		if err = rows.Scan(&id); err != nil {
			return
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	return
}

func (oDb *DB) GetBActionErrors(ctx context.Context) (lines []BActionErrorCount, err error) {
	const query = `SELECT
               e.svc_id,
               e.node_id,
               s.svc_env,
               e.err
             FROM
               b_action_errors e
             JOIN services s ON e.svc_id=s.svc_id`

	var rows *sql.Rows

	rows, err = oDb.DB.QueryContext(ctx, query)
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var line BActionErrorCount
		if err = rows.Scan(&line.SvcID, &line.NodeID, &line.SvcEnv, &line.ErrCount); err != nil {
			return
		}
		lines = append(lines, line)
	}
	err = rows.Err()
	return
}

func (oDb *DB) UpdateUnfinishedActions(ctx context.Context) error {
	request := `UPDATE svcactions
		SET
		    status = "err",
		    end = "1000-01-01 00:00:00"
		WHERE
		    begin < DATE_SUB(NOW(), INTERVAL 120 MINUTE)
		    AND end IS NULL
		    AND status IS NULL
		    AND action NOT LIKE "%#%"`
	result, err := oDb.DB.ExecContext(ctx, request)
	if err != nil {
		return err
	}
	if rowAffected, err := result.RowsAffected(); err != nil {
		return err
	} else if rowAffected > 0 {
		oDb.SetChange("svcactions")
	}
	return nil
}

func (oDb *DB) GetUnfinishedActions(ctx context.Context) (lines []SvcAction, err error) {
	query := `SELECT id, node_id, svc_id FROM svcactions
		WHERE
		    begin < DATE_SUB(NOW(), INTERVAL 120 MINUTE)
		    AND end IS NULL
		    AND status IS NULL
		    AND action NOT LIKE "%#%"`
	var rows *sql.Rows
	rows, err = oDb.DB.QueryContext(ctx, query)
	if err != nil {
		return
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var line SvcAction
		if err = rows.Scan(&line.ID, &line.NodeID, &line.SvcID); err != nil {
			return
		}
		lines = append(lines, line)
	}
	err = rows.Err()
	return

}

func (oDb *DB) InsertSvcAction(ctx context.Context, svcID, nodeID uuid.UUID, action string, begin time.Time, status_log string, sid string, cron bool, end time.Time, status string) (int64, error) {
	query := "INSERT INTO svcactions (svc_id, node_id, action, begin, status_log, sid, cron"
	placeholders := "?, ?, ?, ?, ?, ?, ?"
	args := []any{svcID, nodeID, action, begin, status_log, sid, cron}

	if !end.IsZero() {
		query += ", end"
		placeholders += ", ?"
		args = append(args, end)
	}
	if status != "" {
		query += ", status"
		placeholders += ", ?"
		args = append(args, status)
	}
	query += fmt.Sprintf(") VALUES (%s)", placeholders)

	result, err := oDb.DB.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}

	id, err := result.LastInsertId()
	if err != nil {
		return 0, err
	}

	if rowsAffected, err := result.RowsAffected(); err != nil {
		return id, err
	} else if rowsAffected > 0 {
		oDb.SetChange("svcactions")
	}

	return id, nil
}

func (oDb *DB) UpdateSvcAction(ctx context.Context, svcActionID int64, end time.Time, status, statusLog string) error {
	const query = `UPDATE svcactions SET end = ?, status = ?, time = TIMESTAMPDIFF(SECOND, begin, ?), status_log = ? WHERE id = ?`
	result, err := oDb.DB.ExecContext(ctx, query, end, status, end, statusLog, svcActionID)
	if err != nil {
		return err
	}
	if rowsAffected, err := result.RowsAffected(); err != nil {
		return err
	} else if rowsAffected > 0 {
		oDb.SetChange("svcactions")
	}
	return nil
}

// FindInstanceActionIDFromSID finds the instance action ID from the sid.
func (oDb *DB) FindInstanceActionIDFromSID(ctx context.Context, nodeID string, svcID string, sid string) (id int64, found bool, err error) {
	const query = "SELECT id FROM svcactions WHERE node_id = ? AND svc_id = ? AND sid = ?"
	err = oDb.DB.QueryRowContext(ctx, query, nodeID, svcID, sid).Scan(&id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			err = nil
		}
	} else {
		found = true
	}
	return
}

// UpdateActionErrors updates the action errors in the database.
func (oDb *DB) UpdateActionErrors(ctx context.Context, svcID string, nodeID string) error {
	const queryCount = `SELECT count(id) FROM svcactions a
             WHERE
               a.svc_id = ? AND
               a.node_id = ? AND
               a.status = "err" AND
               ((a.ack <> 1) OR (a.ack IS NULL)) AND
               a.begin > DATE_SUB(NOW(), INTERVAL 2 DAY)`

	var errCount int
	if err := oDb.DB.QueryRowContext(ctx, queryCount, svcID, nodeID).Scan(&errCount); err != nil {
		return fmt.Errorf("UpdateActionErrors: count failed: %w", err)
	}

	if errCount == 0 {
		const queryDelete = `DELETE FROM b_action_errors
                  WHERE
                    svc_id = ? AND
                    node_id = ?`
		if _, err := oDb.DB.ExecContext(ctx, queryDelete, svcID, nodeID); err != nil {
			return fmt.Errorf("UpdateActionErrors: delete failed: %w", err)
		}
	} else {
		const queryInsert = `INSERT INTO b_action_errors
                 SET
                   svc_id= ?,
                   node_id= ?,
                   err= ?
                 ON DUPLICATE KEY UPDATE
                   err= ?`
		if _, err := oDb.DB.ExecContext(ctx, queryInsert, svcID, nodeID, errCount, errCount); err != nil {
			return fmt.Errorf("UpdateActionErrors: insert/update failed: %w", err)
		}
	}
	return nil
}

// UpdateDashActionErrors updates the dashboard with action errors.
func (oDb *DB) UpdateDashActionErrors(ctx context.Context, svcID string, nodeID string) error {
	const query = `SELECT e.err, s.svc_env FROM b_action_errors e
             JOIN services s ON e.svc_id=s.svc_id
             WHERE
               e.svc_id = ? AND
               e.node_id = ?`

	var errCount int
	var svcEnv sql.NullString
	var err error

	err = oDb.DB.QueryRowContext(ctx, query, svcID, nodeID).Scan(&errCount, &svcEnv)
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("UpdateDashActionErrors: select failed: %w", err)
	}

	if err == nil {
		sev := 3
		if svcEnv.Valid && svcEnv.String == "PRD" {
			sev = 4
		}
		const queryInsert = `INSERT INTO dashboard
                 SET
                   dash_type="action errors",
                   svc_id = ?,
                   node_id = ?,
                   dash_severity = ?,
                   dash_fmt = "%(err)s action errors",
                   dash_dict = ?,
                   dash_created = NOW(),
                   dash_updated = NOW(),
                   dash_env = ?
                 ON DUPLICATE KEY UPDATE
                   dash_severity = ?,
                   dash_fmt = "%(err)s action errors",
                   dash_dict = ?,
                   dash_updated = NOW(),
                   dash_env = ?`

		dashDict := fmt.Sprintf(`{"err": "%d"}`, errCount)

		if _, err := oDb.DB.ExecContext(ctx, queryInsert, svcID, nodeID, sev, dashDict, svcEnv, sev, dashDict, svcEnv); err != nil {
			return fmt.Errorf("UpdateDashActionErrors: insert/update failed: %w", err)
		}
		// TODO: WebSocket notification
	} else {
		// TODO: WebSocket notification
		const queryDelete = `DELETE FROM dashboard
                 WHERE
                   dash_type="action errors" AND
                   svc_id = ? AND
                   node_id = ?`
		if _, err := oDb.DB.ExecContext(ctx, queryDelete, svcID, nodeID); err != nil {
			return fmt.Errorf("UpdateDashActionErrors: delete failed: %w", err)
		}
	}
	return nil
}
