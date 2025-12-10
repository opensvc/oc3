package scheduler

import (
	"context"
	"time"
)

var TaskRefreshBActionErrors = Task{
	name:    "refresh_b_action_errors",
	period:  24 * time.Hour,
	fn:      taskRefreshBActionErrorsRun,
	timeout: time.Minute,
}

func taskRefreshBActionErrorsRun(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()

	if err := odb.BActionErrorsRefresh(ctx); err != nil {
		return err
	}

	return odb.Commit()
}
