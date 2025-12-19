package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/opensvc/oc3/cdb"
)

// TaskScrub marks services, resources and instances status "undef"
// When all instances have outdated or absent data.
//
// For testing, force a scrubable dataset with:
//
//	UPDATE services SET svc_status="up" WHERE svc_id IN (SELECT svc_id FROM v_outdated_services);
var TaskScrub = Task{
	name:    "scrub",
	period:  time.Minute,
	fn:      taskScrubRun,
	timeout: time.Minute,
}

var TaskScrubChecks = Task{
	name:    "scrub_checks",
	period:  24 * time.Hour,
	fn:      taskScrubChecks,
	timeout: time.Minute,
}

func taskScrubRun(ctx context.Context, task *Task) (err error) {
	err = errors.Join(err, taskScrubObjects(ctx, task))
	err = errors.Join(err, taskScrubResources(ctx, task))
	err = errors.Join(err, taskScrubInstances(ctx, task))
	return
}

func taskScrubInstances(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()
	instanceIDs, err := odb.InstancesOutdated(ctx)
	if err != nil {
		return err
	}
	if instanceIDs != nil {
		slog.Info(fmt.Sprintf("purge outdated %s", instanceIDs))
	}
	for _, instanceID := range instanceIDs {
		odb.PurgeInstance(ctx, instanceID)
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}

func taskScrubResources(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()

	// Fetch the outdated resources still not in "undef" availstatus
	resources, err := odb.ResourceOutdatedLists(ctx)
	if err != nil {
		return err
	}

	n := len(resources)
	if n == 0 {
		return nil
	}

	names := make([]string, n)

	// Historize `resmon` lines we will touch
	for i, resource := range resources {
		names[i] = resource.String()
		if err := odb.ResourceUpdateLog(ctx, resource, "undef"); err != nil {
			return err
		}
	}

	// Update the `resmon` table
	if modified, err := odb.ResourceUpdateStatus(ctx, resources, "undef"); err != nil {
		return err
	} else if int(modified) != n {
		task.Infof("set %d/%d resmon status to undef (no live instance) amongst %s", modified, n, names)
	} else {
		task.Infof("set %d resmon status to undef (no live instance) for %s", n, names)
	}

	// Create log table entries
	logEntries := make([]cdb.LogEntry, n)
	for i, resource := range resources {
		d := make(map[string]any)
		d["name"] = resource.String()
		logEntries[i] = cdb.LogEntry{
			Action: "resource.status",
			Fmt:    "resource '%(name)s' status flagged 'undef'",
			Dict:   d,
			User:   "scheduler",
			Level:  "error",
			SvcID:  &resource.OID,
			NodeID: &resource.NID,
		}
	}
	if err := odb.Log(ctx, logEntries...); err != nil {
		return err
	}
	odb.Session.SetChanges("log")

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}

	return odb.Commit()
}

func taskScrubObjects(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()

	// Fetch the outdated services still not in "undef" availstatus
	objects, err := odb.ObjectsOutdated(ctx)
	if err != nil {
		return err
	}

	n := len(objects)
	if n == 0 {
		return nil
	}

	// Historize `services` lines we will touch
	for _, o := range objects {
		if err := odb.ObjectUpdateLog(ctx, o.OID.String(), "undef"); err != nil {
			return err
		}
	}

	// Update the `services` table
	if modified, err := odb.ObjectUpdateStatusSimple(ctx, objects, "undef", "undef"); err != nil {
		return err
	} else if int(modified) != n {
		task.Infof("set %d/%d services status to undef (no live instance) amongst %s", modified, n, objects)
	} else {
		task.Infof("set %d services status to undef (no live instance) for %s", n, objects)
	}

	// Create log table entries
	logEntries := make([]cdb.LogEntry, n)
	for i, o := range objects {
		d := make(map[string]any)
		d["svc"] = o.String()
		logEntries[i] = cdb.LogEntry{
			Action: "service.status",
			Fmt:    "service '%(svc)s' has zero live instance. Status flagged 'undef'",
			Dict:   d,
			User:   "scheduler",
			Level:  "error",
			SvcID:  &o.OID,
		}
	}
	if err := odb.Log(ctx, logEntries...); err != nil {
		return err
	}
	odb.Session.SetChanges("log")

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}

	return odb.Commit()
}

func taskScrubChecks(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()

	if err := odb.PurgeChecksOutdated(ctx); err != nil {
		return err
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}
