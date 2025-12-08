package scheduler

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/opensvc/oc3/cdb"
)

// TaskScrub marks services, resources and instances status "undef"
// When all instances have outdated or absent data.
//
// For testing, force a scrubable dataset with:
//
//	UPDATE services SET svc_status="up" WHERE svc_id IN (SELECT svc_id FROM v_outdated_services);
var TaskScrub = Task{
	name:   "scrub",
	period: time.Minute,
	fn:     taskScrubRun,
}

func taskScrubRun(ctx context.Context, task *Task) (err error) {
	err = errors.Join(err, taskScrubRunSvcStatus(ctx, task))
	err = errors.Join(err, taskScrubRunResStatus(ctx, task))
	err = errors.Join(err, taskScrubRunSvcInstances(ctx, task))
	return
}

func taskScrubRunSvcStatus(ctx context.Context, task *Task) error {
	return nil
}

func taskScrubRunResStatus(ctx context.Context, task *Task) error {
	return nil
}

func taskScrubRunSvcInstances(ctx context.Context, task *Task) error {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	tx, err := task.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	sql := `SELECT id, svc_id, svcname, svc_availstatus, svc_status_updated FROM services
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
	entries := make([]cdb.ServiceLogUpdate, 0)
	now := time.Now()

	for {
		next := rows.Next()
		if !next {
			break
		}
		var entry cdb.ServiceLogUpdate
		var svcID uuid.UUID
		var svcName string
		var id int64
		rows.Scan(&id, &svcID, &svcName, &entry.AvailStatus, &entry.End)
		svcIDs = append(svcIDs, svcID)
		svcNames = append(svcNames, svcName)
		ids = append(ids, id)
		entry.SvcID = svcID
		entry.Begin = now
		entry.End = now
		entries = append(entries, entry)
	}
	n := len(ids)
	if n == 0 {
		return nil
	}

	if err := cdb.UpdateServicesLog(ctx, tx, entries...); err != nil {
		return err
	}

	sql = `UPDATE services
		SET svc_status = "undef", svc_availstatus="undef"
		WHERE id IN (%s)`
	sql = fmt.Sprintf(sql, cdb.Placeholders(n))

	result, err := tx.ExecContext(ctx, sql, ids...)
	if err != nil {
		return err
	}
	if n, err := result.RowsAffected(); err != nil {
		return err
	} else if n > 0 {
		task.Session().SetChanges("services")
	}
	task.Infof("set %d services status to undef (no live instance) for %s", n, svcIDs)
	logEntries := make([]cdb.LogEntry, n)
	for i, svcID := range svcIDs {
		d := make(map[string]any)
		d["svc"] = svcNames[i]
		logEntries[i] = cdb.LogEntry{
			Action: "service.status",
			Fmt:    "service '%(svc)s' has zero live instance. Status flagged 'undef'",
			Dict:   d,
			User:   "scheduler",
			Level:  "error",
			SvcID:  &svcID,
		}
	}
	if err := cdb.Log(ctx, tx, logEntries...); err != nil {
		return err
	}
	task.session.SetChanges("log")
	if err := tx.Commit(); err != nil {
		return err
	}
	task.session.NotifyChanges(ctx)
	return nil
}
