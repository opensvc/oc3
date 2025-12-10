package scheduler

import (
	"context"
	"errors"
	"fmt"
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

func taskScrubRun(ctx context.Context, task *Task) (err error) {
	err = errors.Join(err, taskScrubRunSvcStatus(ctx, task))
	err = errors.Join(err, taskScrubRunResStatus(ctx, task))
	err = errors.Join(err, taskScrubRunSvcInstances(ctx, task))
	return
}

func taskScrubRunSvcInstances(ctx context.Context, task *Task) error {
	return nil
}

func taskScrubRunResStatus(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()

	// Fetch the outdated resources still not in "undef" availstatus
	ids, rids, svcIDs, nodeIDs, err := odb.ResourceOutdatedLists(ctx)
	if err != nil {
		return err
	}

	n := len(rids)
	if n == 0 {
		return nil
	}
	names := make([]string, n)

	// Historize `services` lines we will touch
	for i, _ := range ids {
		names[i] = fmt.Sprintf("%s@%s:%s", svcIDs[i], nodeIDs[i], rids[i])
		if err := odb.ResourceUpdateLog(ctx, rids[i], svcIDs[i], nodeIDs[i], "undef"); err != nil {
			return err
		}
	}

	// Update the `services` table
	if n, err := odb.ResourceUpdateStatus(ctx, ids, "undef"); err != nil {
		return err
	} else if int(n) != len(ids) {
		task.Infof("set %d/%d resmon status to undef (no live instance) amongst %s", n, len(svcIDs), names)
	} else {
		task.Infof("set %d resmon status to undef (no live instance) for %s", n, names)
	}

	// Create log table entries
	logEntries := make([]cdb.LogEntry, n)
	for i, _ := range ids {
		d := make(map[string]any)
		d["name"] = names[i]
		logEntries[i] = cdb.LogEntry{
			Action: "resource.status",
			Fmt:    "resource '%(name)s' status flagged 'undef'",
			Dict:   d,
			User:   "scheduler",
			Level:  "error",
			SvcID:  &svcIDs[i],
			NodeID: &nodeIDs[i],
		}
	}
	if err := odb.Log(ctx, logEntries...); err != nil {
		return err
	}
	odb.Session.SetChanges("log")

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}

	// Commit and notify client of changed tables
	if err := odb.Commit(); err != nil {
		return err
	}
	return nil
}

func taskScrubRunSvcStatus(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()

	// Fetch the outdated services still not in "undef" availstatus
	ids, svcIDs, svcNames, err := odb.ObjectOutdatedLists(ctx)
	if err != nil {
		return err
	}

	n := len(ids)
	if n == 0 {
		return nil
	}

	// Historize `services` lines we will touch
	for _, svcID := range svcIDs {
		if err := odb.ObjectUpdateLog(ctx, svcID.String(), "undef"); err != nil {
			return err
		}
	}

	// Update the `services` table
	if n, err := odb.ObjectUpdateStatusSimple(ctx, ids, "undef", "undef"); err != nil {
		return err
	} else if int(n) != len(svcIDs) {
		task.Infof("set %d/%d services status to undef (no live instance) amongst %s", n, len(svcIDs), svcIDs)
	} else {
		task.Infof("set %d services status to undef (no live instance) for %s", n, svcIDs)
	}

	// Create log table entries
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
	if err := odb.Log(ctx, logEntries...); err != nil {
		return err
	}
	odb.Session.SetChanges("log")

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}

	// Commit and notify client of changed tables
	if err := odb.Commit(); err != nil {
		return err
	}
	return nil
}
