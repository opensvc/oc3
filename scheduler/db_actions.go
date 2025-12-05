package scheduler

import (
	"context"
	"database/sql"
	"time"
)

func TaskRefreshBActionErrors(ctx context.Context, task *Task, db *sql.DB) error {

	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, "TRUNCATE b_action_errors")
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

	_, err = tx.ExecContext(ctx, sql)
	if err != nil {
		return err
	}

	return tx.Commit()
}
