package scheduler

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/spf13/viper"
)

var TaskTrim = Task{
	name:   "trim",
	period: time.Minute,
	fn:     taskTrimRun,
}

// deleteBatched executes the deletion query in batches until no rows are affected.
func deleteBatched(ctx context.Context, task *Task, table, dateCol, orderbyCol string) error {
	batchSize := getBatchSize(table)
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
		result, err := task.db.ExecContext(ctx, query)
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

func getBatchSize(table string) int64 {
	n := viper.GetInt64(fmt.Sprintf("scheduler.task.trim.table.%s.batch_size", table))
	if n == 0 {
		n = viper.GetInt64("scheduler.task.trim.batch_size")
	}
	return n
}

func getRetentionDays(table string) int {
	days := viper.GetInt(fmt.Sprintf("scheduler.task.trim.table.%s.retention", table))
	if days == 0 {
		days = viper.GetInt("scheduler.task.trim.retention")
	}
	return days
}

func taskTrimRun(ctx context.Context, task *Task) (err error) {
	err = errors.Join(err, deleteBatched(ctx, task, "saves", "save_date", "id"))
	err = errors.Join(err, deleteBatched(ctx, task, "log", "log_date", "id"))
	err = errors.Join(err, deleteBatched(ctx, task, "stat_day_disk_array", "day", "id"))
	err = errors.Join(err, deleteBatched(ctx, task, "stat_day_disk_array_dg", "day", "id"))
	err = errors.Join(err, deleteBatched(ctx, task, "stat_day_disk_app", "day", "id"))
	err = errors.Join(err, deleteBatched(ctx, task, "stat_day_disk_app_dg", "day", "id"))
	err = errors.Join(err, deleteBatched(ctx, task, "switches", "sw_updated", "id"))
	err = errors.Join(err, deleteBatched(ctx, task, "comp_log", "run_date", "id"))
	err = errors.Join(err, deleteBatched(ctx, task, "comp_log_daily", "run_date", "id"))
	err = errors.Join(err, deleteBatched(ctx, task, "resmon_log", "res_end", "id"))
	err = errors.Join(err, deleteBatched(ctx, task, "svcmon_log", "mon_end", "id"))
	err = errors.Join(err, deleteBatched(ctx, task, "svcactions", "begin", "id"))
	err = errors.Join(err, deleteBatched(ctx, task, "dashboard_events", "dash_end", "id"))
	err = errors.Join(err, deleteBatched(ctx, task, "packages", "pkg_updated", "id"))
	err = errors.Join(err, deleteBatched(ctx, task, "patches", "patch_updated", "id"))
	err = errors.Join(err, deleteBatched(ctx, task, "node_ip", "updated", "id"))
	err = errors.Join(err, deleteBatched(ctx, task, "node_users", "updated", "id"))
	err = errors.Join(err, deleteBatched(ctx, task, "node_groups", "updated", "id"))
	err = errors.Join(err, deleteBatched(ctx, task, "comp_run_ruleset", "date", "id"))
	err = errors.Join(err, deleteBatched(ctx, task, "links", "link_last_consultation_date", "id"))
	err = errors.Join(err, deleteBatched(ctx, task, "services_log", "svc_end", "id"))
	return
}
