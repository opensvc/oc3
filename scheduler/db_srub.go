package scheduler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/opensvc/oc3/cdb"
)

func TaskScrub(ctx context.Context, task *Task, db *sql.DB) (err error) {
	err = errors.Join(err, TaskScrubSvcStatus(ctx, task, db))
	err = errors.Join(err, TaskScrubResStatus(ctx, task, db))
	err = errors.Join(err, TaskScrubSvcInstances(ctx, task, db))
	return
}

func TaskScrubSvcStatus(ctx context.Context, task *Task, db *sql.DB) error {
	return nil
}

func TaskScrubResStatus(ctx context.Context, task *Task, db *sql.DB) error {
	return nil
}

func Placeholders(n int) string {
	return strings.TrimRight(strings.Repeat("?,", n), ",")
}

func TaskScrubSvcInstances(ctx context.Context, task *Task, db *sql.DB) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	sql := `SELECT id, svc_id, svcname FROM services
		WHERE svc_id IN (SELECT svc_id FROM v_outdated_services)
                AND (svc_status != "undef" OR svc_availstatus != "undef")`
	rows, err := tx.QueryContext(ctx, sql)
	if err != nil {
		return err
	}
	defer rows.Close()

	svcIDs := make([]uuid.UUID, 0)
	svcNames := make([]string, 0)
	ids := make([]any, 0)

	for {
		next := rows.Next()
		if !next {
			break
		}
		var svcID uuid.UUID
		var svcName string
		var id int64
		rows.Scan(&id, &svcID, &svcName)
		svcIDs = append(svcIDs, svcID)
		svcNames = append(svcNames, svcName)
		ids = append(ids, id)
	}

	n := len(ids)
	if n == 0 {
		return nil
	}

	sql = `UPDATE services
		SET svc_status = "undef", svc_availstatus="undef"
		WHERE id IN (%s)`
	sql = fmt.Sprintf(sql, Placeholders(len(ids)))

	_, err = tx.ExecContext(ctx, sql, ids...)
	if err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	task.Infof("set %d services status to undef (no live instance) for %s", n, svcIDs)
	entries := make([]cdb.LogEntry, n)
	for i, svcID := range svcIDs {
		d := make(map[string]any)
		d["svc"] = svcNames[i]
		entries[i] = cdb.LogEntry{
			Action: "service.status",
			Fmt:    "service '%(svc)s' has zero live instance. Status flagged 'undef'",
			Dict:   d,
			User:   "scheduler",
			Level:  "error",
			SvcID:  &svcID,
		}
	}
	err = cdb.Log(ctx, db, entries...)
	return err
}
