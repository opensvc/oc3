package cdb

import (
	"context"
	"database/sql"
)

type (
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
               a.end>date_sub(now(), interval 1 day) AND
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

func (oDb *DB) AlertActionErrors(ctx context.Context, line BActionErrorCount) error {
	var (
		env      string
		severity int
	)
	if line.SvcEnv != nil && *line.SvcEnv == "PRD" {
		env = *line.SvcEnv
		severity = 4
	} else {
		env = "TST"
		severity = 3
	}
	request := `
                 INSERT INTO dashboard
                 SET
                   dash_type="action errors",
                   svc_id=?,
                   node_id=?,
                   dash_severity=?,
                   dash_fmt="%(err)s action errors",
                   dash_dict=CONCAT('{"err": "', ?, '"}'),
                   dash_created=NOW(),
                   dash_env=?,
                   dash_updated=NOW()
                 ON DUPLICATE KEY UPDATE
                   dash_severity=?,
                   dash_fmt="%(err)s action errors",
                   dash_dict=CONCAT('{"err": "', ?, '"}'),
                   dash_updated=NOW()`
	result, err := oDb.DB.ExecContext(ctx, request, line.SvcID, line.NodeID, severity, line.ErrCount, env, severity, line.ErrCount)
	if err != nil {
		return err
	}
	if rowAffected, err := result.RowsAffected(); err != nil {
		return err
	} else if rowAffected > 0 {
		oDb.SetChange("dashboard")
	}
	return nil
}
