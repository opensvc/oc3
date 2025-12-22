package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/opensvc/oc3/cdb"
)

var TaskAlertHourly = Task{
	name: "alerts_hourly",
	children: TaskList{
		TaskAlertNetworkWithWrongMask,
		TaskAlertInstancesOutdated,
	},
	period:  time.Hour,
	timeout: 15 * time.Minute,
}

var TaskAlertDaily = Task{
	name: "alerts_daily",
	children: TaskList{
		TaskAlertAppsWithoutResponsible,
	},
	period:  24 * time.Hour,
	timeout: 15 * time.Minute,
}

var TaskAlertNetworkWithWrongMask = Task{
	name:    "alert_network_with_wrong_mask",
	fn:      taskAlertNetworkWithWrongMask,
	timeout: 5 * time.Minute,
}

var TaskAlertAppsWithoutResponsible = Task{
	name:    "alert_apps_without_responsible",
	fn:      taskAlertAppWithoutResponsible,
	timeout: 5 * time.Minute,
}

var TaskAlertInstancesOutdated = Task{
	name:    "alert_instances_outdated",
	fn:      taskAlertInstancesOutdated,
	timeout: 5 * time.Minute,
}

func taskAlertNetworkWithWrongMask(ctx context.Context, task *Task) error {
	var severity int
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()
	lines, err := odb.NetworksWithWrongMask(ctx)
	if err != nil {
		return err
	}
	for _, line := range lines {
		if line.NetMask == nil {
			continue
		}
		if line.NodeEnv == "PRD" {
			severity = 4
		} else {
			severity = 3
		}
		dict := fmt.Sprintf(`{"addr": "%s", "mask": "%d", "net_netmask": "%d"}`, line.Addr, line.NodeMask, *line.NetMask)
		slog.Debug(fmt.Sprintf("alert: netmask misconfigured: %s: %s configured with mask %d instead of %d", line.NodeID, line.Addr, line.NodeMask, *line.NetMask))
		odb.DashboardUpdateObject(ctx, &cdb.Dashboard{
			NodeID:   line.NodeID,
			ObjectID: "",
			Type:     "netmask misconfigured",
			Fmt:      "%(addr)s configured with mask %(mask)s instead of %(net_netmask)s",
			Dict:     dict,
			Severity: severity,
			Env:      line.NodeEnv,
		})
	}
	if err := odb.DashboardDeleteNetworkWrongMaskNotUpdated(ctx); err != nil {
		return err
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}

func taskAlertAppWithoutResponsible(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()
	apps, err := odb.AppsWithoutResponsible(ctx)
	if err != nil {
		return err
	}
	if len(apps) == 0 {
		return nil
	}
	odb.Log(ctx, cdb.LogEntry{
		Action: "app",
		Fmt:    "applications with no declared responsibles %(app)s",
		Dict: map[string]any{
			"app": strings.Join(apps, ", "),
		},
		User:  "scheduler",
		Level: "warning",
	})
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}

func taskAlertInstancesOutdated(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()
	if err := odb.AlertInstancesOutdated(ctx); err != nil {
		return err
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}
