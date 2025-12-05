package scheduler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/spf13/viper"
)

// deleteBatched executes the deletion query in batches until no rows are affected.
func deleteBatched(ctx context.Context, task *Task, db *sql.DB, table, dateCol, orderbyCol string) error {
	batchSize := viper.GetInt64("scheduler.task.trim.default.batch_size")
	retention := getRetentionDays(table)

	// The base SQL query for the batched deletion.
	// ORDER BY is crucial for consistent performance and avoiding lock conflicts.
	query := fmt.Sprintf("DELETE FROM `%s` WHERE `%s` < DATE_SUB(NOW(), INTERVAL %d DAY) ORDER BY `%s` LIMIT %d",
		table, dateCol, retention, orderbyCol, batchSize)

	totalDeleted := 0
	batchCount := 0

	for {
		batchCount++

		ctx, cancel := context.WithTimeout(ctx, time.Minute)

		// Execute the DELETE statement
		result, err := db.ExecContext(ctx, query)
		cancel()
		if err != nil {
			task.Errorf("%s: error executing batch %d: %v", table, batchCount, err)
			return err
		}

		// Check the number of affected rows
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			task.Errorf("%s: error checking affected rows for batch %d: %v", table, batchCount, err)
			return err
		}

		totalDeleted += int(rowsAffected)
		if rowsAffected > 0 {
			task.Infof("%s: batch %d: deleted %d rows. total deleted: %d", table, batchCount, rowsAffected, totalDeleted)
		}

		// If less than the batch size was deleted, we've reached the end of the matching rows.
		if rowsAffected < batchSize {
			task.Infof("%s: deletion complete. retention: %d days. batch size: %d. total batches: %d. total rows deleted: %d", table, retention, batchSize, batchCount, totalDeleted)
			return nil
		}

		// Add a short sleep to yield CPU time, preventing resource monopolization
		time.Sleep(10 * time.Millisecond)
	}
}

func getRetentionDays(table string) int {
	days := viper.GetInt(fmt.Sprintf("scheduler.task.trim.%s.retention", table))
	if days == 0 {
		days = viper.GetInt("scheduler.task.trim.default.retention")
	}
	return days
}

func TaskTrimSaves(ctx context.Context, task *Task, db *sql.DB) error {
	return deleteBatched(ctx, task, db, "saves", "save_date", "id")
}

func TaskTrimLog(ctx context.Context, task *Task, db *sql.DB) error {
	return deleteBatched(ctx, task, db, "log", "log_date", "id")
}

func TaskTrimStatDayDiskArray(ctx context.Context, task *Task, db *sql.DB) error {
	return deleteBatched(ctx, task, db, "stat_day_disk_array", "day", "id")
}

func TaskTrimStatDayDiskArrayDG(ctx context.Context, task *Task, db *sql.DB) error {
	return deleteBatched(ctx, task, db, "stat_day_disk_array_dg", "day", "id")
}

func TaskTrimStatDayDiskApp(ctx context.Context, task *Task, db *sql.DB) error {
	return deleteBatched(ctx, task, db, "stat_day_disk_app", "day", "id")
}

func TaskTrimStatDayDiskAppDG(ctx context.Context, task *Task, db *sql.DB) error {
	return deleteBatched(ctx, task, db, "stat_day_disk_app_dg", "day", "id")
}

func TaskTrimSwitches(ctx context.Context, task *Task, db *sql.DB) error {
	return deleteBatched(ctx, task, db, "switches", "sw_updated", "id")
}

func TaskTrimCompLog(ctx context.Context, task *Task, db *sql.DB) error {
	return deleteBatched(ctx, task, db, "comp_log", "run_date", "id")
}

func TaskTrimCompLogDaily(ctx context.Context, task *Task, db *sql.DB) error {
	return deleteBatched(ctx, task, db, "comp_log_daily", "run_date", "id")
}

func TaskTrimResmonLog(ctx context.Context, task *Task, db *sql.DB) error {
	return deleteBatched(ctx, task, db, "resmon_log", "res_end", "id")
}

func TaskTrimSvcmonLog(ctx context.Context, task *Task, db *sql.DB) error {
	return deleteBatched(ctx, task, db, "svcmon_log", "mon_end", "id")
}

func TaskTrimSvcactions(ctx context.Context, task *Task, db *sql.DB) error {
	return deleteBatched(ctx, task, db, "svcactions", "begin", "id")
}

func TaskTrimDashboardEvents(ctx context.Context, task *Task, db *sql.DB) error {
	return deleteBatched(ctx, task, db, "dashboard_events", "dash_end", "id")
}

func TaskTrimPackages(ctx context.Context, task *Task, db *sql.DB) error {
	return deleteBatched(ctx, task, db, "packages", "pkg_updated", "id")
}

func TaskTrimPatches(ctx context.Context, task *Task, db *sql.DB) error {
	return deleteBatched(ctx, task, db, "patches", "patch_updated", "id")
}

func TaskTrimNodeIP(ctx context.Context, task *Task, db *sql.DB) error {
	return deleteBatched(ctx, task, db, "node_ip", "updated", "id")
}

func TaskTrimNodeUsers(ctx context.Context, task *Task, db *sql.DB) error {
	return deleteBatched(ctx, task, db, "node_users", "updated", "id")
}

func TaskTrimNodeGroups(ctx context.Context, task *Task, db *sql.DB) error {
	return deleteBatched(ctx, task, db, "node_groups", "updated", "id")
}

func TaskTrimCompRunRuleset(ctx context.Context, task *Task, db *sql.DB) error {
	return deleteBatched(ctx, task, db, "comp_run_ruleset", "date", "id")
}

func TaskTrimLinks(ctx context.Context, task *Task, db *sql.DB) error {
	return deleteBatched(ctx, task, db, "links", "link_last_consultation_date", "id")
}

func TaskTrimServicesLog(ctx context.Context, task *Task, db *sql.DB) error {
	return deleteBatched(ctx, task, db, "services_log", "svc_end", "id")
}

func TaskTrim(ctx context.Context, task *Task, db *sql.DB) (err error) {
	err = errors.Join(err, TaskTrimSaves(ctx, task, db))
	err = errors.Join(err, TaskTrimLog(ctx, task, db))
	err = errors.Join(err, TaskTrimStatDayDiskArray(ctx, task, db))
	err = errors.Join(err, TaskTrimStatDayDiskArrayDG(ctx, task, db))
	err = errors.Join(err, TaskTrimStatDayDiskApp(ctx, task, db))
	err = errors.Join(err, TaskTrimStatDayDiskAppDG(ctx, task, db))
	err = errors.Join(err, TaskTrimSwitches(ctx, task, db))
	err = errors.Join(err, TaskTrimCompLog(ctx, task, db))
	err = errors.Join(err, TaskTrimCompLogDaily(ctx, task, db))
	err = errors.Join(err, TaskTrimResmonLog(ctx, task, db))
	err = errors.Join(err, TaskTrimSvcmonLog(ctx, task, db))
	err = errors.Join(err, TaskTrimSvcactions(ctx, task, db))
	err = errors.Join(err, TaskTrimDashboardEvents(ctx, task, db))
	err = errors.Join(err, TaskTrimPackages(ctx, task, db))
	err = errors.Join(err, TaskTrimPatches(ctx, task, db))
	err = errors.Join(err, TaskTrimNodeIP(ctx, task, db))
	err = errors.Join(err, TaskTrimNodeUsers(ctx, task, db))
	err = errors.Join(err, TaskTrimNodeGroups(ctx, task, db))
	err = errors.Join(err, TaskTrimCompRunRuleset(ctx, task, db))
	err = errors.Join(err, TaskTrimLinks(ctx, task, db))
	err = errors.Join(err, TaskTrimServicesLog(ctx, task, db))
	return
}
