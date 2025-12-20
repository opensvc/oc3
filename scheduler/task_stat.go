package scheduler

import (
	"context"
	"time"
)

var TaskStatDiskDaily = Task{
	name:   "stat_disk_daily",
	period: 24 * time.Hour,
	children: TaskList{
		TaskStatDiskAppDaily,
		TaskStatDiskAppDGDaily,
		TaskStatDiskArrayDaily,
		TaskStatDiskArrayDGDaily,
	},
	timeout: 5 * time.Minute,
}

var TaskStatDiskAppDaily = Task{
	name:    "stat_disk_app_daily",
	fn:      taskStatDiskAppDaily,
	timeout: 5 * time.Minute,
}

var TaskStatDiskAppDGDaily = Task{
	name:    "stat_disk_app_dg_daily",
	fn:      taskStatDiskAppDGDaily,
	timeout: 5 * time.Minute,
}

var TaskStatDiskArrayDaily = Task{
	name:    "stat_disk_array_daily",
	fn:      taskStatDiskArrayDaily,
	timeout: 5 * time.Minute,
}

var TaskStatDiskArrayDGDaily = Task{
	name:    "stat_disk_array_dg_daily",
	fn:      taskStatDiskArrayDGDaily,
	timeout: 5 * time.Minute,
}

func taskStatDiskAppDaily(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()
	if err := odb.StatDayDiskApp(ctx); err != nil {
		return err
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}

func taskStatDiskAppDGDaily(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()
	if err := odb.StatDayDiskAppDG(ctx); err != nil {
		return err
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}
func taskStatDiskArrayDaily(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()
	if err := odb.StatDayDiskArray(ctx); err != nil {
		return err
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}

func taskStatDiskArrayDGDaily(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()
	if err := odb.StatDayDiskArrayDG(ctx); err != nil {
		return err
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}
