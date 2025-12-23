package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/opensvc/oc3/cdb"
)

// TaskScrubObjects marks services status "undef" if all instances have outdated or absent data.
//
// For testing, force a scrubable dataset with:
//
//	UPDATE services SET svc_status="up" WHERE svc_id IN (SELECT svc_id FROM v_outdated_services);
var TaskScrubObjects = Task{
	name:    "scrub_object",
	fn:      taskScrubObjects,
	timeout: time.Minute,
}

var TaskScrubResources = Task{
	name:    "scrub_resources",
	fn:      taskScrubResources,
	timeout: time.Minute,
}

var TaskScrubInstances = Task{
	name:    "scrub_instances",
	fn:      taskScrubInstances,
	timeout: time.Minute,
}

var TaskScrubChecksLive = Task{
	name:    "scrub_checks_live",
	fn:      taskScrubChecksLive,
	timeout: time.Minute,
}

var TaskScrubNodeHBA = Task{
	name:    "scrub_node_hba",
	fn:      taskScrubNodeHBA,
	timeout: time.Minute,
}

var TaskScrubSvcdisks = Task{
	name:    "scrub_svcdisks",
	fn:      taskScrubSvcdisks,
	timeout: time.Minute,
}

var TaskScrubDaily = Task{
	name:   "scrub_daily",
	period: 24 * time.Hour,
	children: TaskList{
		TaskScrubChecksLive,
		TaskScrubNodeHBA,
		TaskScrubSvcdisks,
	},
	timeout: 5 * time.Minute,
}

var TaskScrubMinutely = Task{
	name:   "scrub_minutely",
	period: time.Minute,
	children: TaskList{
		TaskScrubObjects,
		TaskScrubResources,
		TaskScrubInstances,
	},
	timeout: time.Minute,
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

func taskScrubChecksLive(ctx context.Context, task *Task) error {
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

func taskScrubNodeHBA(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()

	if err := odb.PurgeNodeHBAsOutdated(ctx); err != nil {
		return err
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}

func taskScrubSvcdisks(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()

	if err := odb.PurgeSvcdisksOutdated(ctx); err != nil {
		return err
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}
