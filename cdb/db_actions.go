package cdb

import "context"

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
