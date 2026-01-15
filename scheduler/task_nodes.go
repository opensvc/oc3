package scheduler

import (
	"context"
	"time"
)

var TaskUpdateVirtualAssets = Task{
	name:    "update_virtual_assets",
	fn:      taskUpdateVirtualAssets,
	timeout: 10 * time.Second,
}

func taskUpdateVirtualAssets(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()
	if err := odb.UpdateVirtualAssets(ctx); err != nil {
		return err
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}
