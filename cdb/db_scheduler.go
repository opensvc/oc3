package cdb

import (
	"context"
	"fmt"
)

// schedulerTasksFrom is the derived table of the scheduler tasks lists, named
// after oc3_scheduler: the tasks and subtasks the scheduler declared, with
// their next run and the counts of their runs of the last 24 hours.
const schedulerTasksFrom = `(SELECT s.id, s.task_name, s.parent, s.task_desc, s.period_s, s.timeout_s,
		s.is_disabled, s.declared, s.last_run_at, s.last_status, s.last_begin_at, s.last_duration_ms, s.last_error,
		IF(s.parent = '' AND s.period_s > 0 AND NOT s.is_disabled AND s.last_run_at IS NOT NULL,
			s.last_run_at + INTERVAL s.period_s SECOND, NULL) AS next_run_at,
		IFNULL(r.runs, 0) AS runs_24h, IFNULL(r.failures, 0) AS failures_24h,
		r.avg_duration_ms AS avg_duration_ms_24h, r.max_duration_ms AS max_duration_ms_24h
	FROM oc3_scheduler s
	LEFT JOIN (
		SELECT task_name, COUNT(*) AS runs, SUM(status = 'failed') AS failures,
			ROUND(AVG(duration_ms)) AS avg_duration_ms, MAX(duration_ms) AS max_duration_ms
		FROM oc3_scheduler_runs
		WHERE begin_at > NOW() - INTERVAL 1 DAY
		GROUP BY task_name
	) r ON r.task_name = s.task_name) oc3_scheduler`

// GetSchedulerTasks lists the scheduler tasks and subtasks, a subtask after
// its task.
func (oDb *DB) GetSchedulerTasks(ctx context.Context, p ListParams) ([]map[string]any, error) {
	return oDb.listQuery(ctx, "GetSchedulerTasks", schedulerTasksFrom, nil, nil,
		"IF(oc3_scheduler.parent = '', oc3_scheduler.task_name, oc3_scheduler.parent), oc3_scheduler.parent != '', oc3_scheduler.task_name", p)
}

// GetSchedulerTask returns a scheduler task or subtask.
func (oDb *DB) GetSchedulerTask(ctx context.Context, name string, p ListParams) ([]map[string]any, error) {
	return oDb.listQuery(ctx, "GetSchedulerTask", schedulerTasksFrom, []string{"oc3_scheduler.task_name = ?"}, []any{name},
		"oc3_scheduler.task_name", p)
}

// GetSchedulerRuns lists the runs of the scheduler tasks, the last first.
func (oDb *DB) GetSchedulerRuns(ctx context.Context, p ListParams) ([]map[string]any, error) {
	return oDb.listQuery(ctx, "GetSchedulerRuns", "oc3_scheduler_runs", nil, nil,
		"oc3_scheduler_runs.id DESC", p)
}

// SchedulerTaskParent returns the task running the subtask name, empty for a
// task, and whether the scheduler declared name.
func (oDb *DB) SchedulerTaskParent(ctx context.Context, name string) (parent string, found bool, err error) {
	rows, err := oDb.DB.QueryContext(ctx, "SELECT parent FROM oc3_scheduler WHERE task_name = ? AND declared = 1", name)
	if err != nil {
		return "", false, fmt.Errorf("SchedulerTaskParent: %w", err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		return "", false, rows.Err()
	}
	if err := rows.Scan(&parent); err != nil {
		return "", false, fmt.Errorf("SchedulerTaskParent scan: %w", err)
	}
	return parent, true, rows.Err()
}

// SetSchedulerTaskDisabled disables or enables a task. The scheduler reads
// the flag every 5 seconds: it stops a disabled task, interrupting its run,
// and starts an enabled one, at the end of its period.
func (oDb *DB) SetSchedulerTaskDisabled(ctx context.Context, name string, disabled bool) error {
	if _, err := oDb.ExecContext(ctx, "UPDATE oc3_scheduler SET is_disabled = ? WHERE task_name = ?", disabled, name); err != nil {
		return fmt.Errorf("SetSchedulerTaskDisabled: %w", err)
	}
	oDb.SetChange("oc3_scheduler")
	return nil
}
