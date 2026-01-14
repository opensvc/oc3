package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/opensvc/oc3/cdb"
)

var TaskAlert1M = Task{
	name: "alerts_1m",
	children: TaskList{
		TaskAlertInstancesNotUpdated,
	},
	period:  time.Minute,
	timeout: 15 * time.Minute,
}

var TaskAlert1H = Task{
	name: "alerts_1h",
	children: TaskList{
		TaskAlertActionErrorCleanup,
		TaskAlertAppsWithoutResponsible,
		TaskAlertChecksNotUpdated,
		TaskAlertNetworkWithWrongMask,
		TaskAlertNodesNotUpdated,
		TaskAlertServiceConfigNotUpdated,
		TaskLogAppsWithoutResponsible,
		TaskLogInstancesNotUpdated,
		TaskPurgeAlertOnDeletedNodes,
		TaskPurgeAlertOnDeletedServices,
	},
	period:  time.Hour,
	timeout: 15 * time.Minute,
}

var TaskAlert1D = Task{
	name: "alerts_1d",
	children: TaskList{
		TaskAlertActionErrorsNotAcked,
		TaskAlertMACDup,
		TaskAlertNodeCloseToMaintenanceEnd,
		TaskAlertNodeMaintenanceExpired,
		TaskAlertNodeWithoutMaintenanceEnd,
		TaskAlertPackageDifferencesInCluster,
		TaskAlertPurgeActionErrors,
		TaskPurgeAlertsOnDeletedInstances,
	},
	period:  24 * time.Hour,
	timeout: 15 * time.Minute,
}

var TaskAlertNodeWithoutMaintenanceEnd = Task{
	name:    "alert_node_without_maintenance_end",
	fn:      taskAlertNodeWithoutMaintenanceEnd,
	timeout: time.Minute,
}

var TaskAlertNodeCloseToMaintenanceEnd = Task{
	name:    "alert_node_close_to_maintenance_end",
	fn:      taskAlertNodeCloseToMaintenanceEnd,
	timeout: time.Minute,
}

var TaskAlertNodeMaintenanceExpired = Task{
	name:    "alert_node_maintenance_expired",
	fn:      taskAlertNodeMaintenanceExpired,
	timeout: time.Minute,
}

var TaskAlertActionErrorCleanup = Task{
	name:    "alert_action_error_cleanup",
	fn:      taskAlertActionErrorCleanup,
	timeout: time.Minute,
}

var TaskAlertNodesNotUpdated = Task{
	name:    "alert_nodes_not_updated",
	fn:      taskAlertNodesNotUpdated,
	timeout: time.Minute,
}

var TaskPurgeAlertsOnDeletedInstances = Task{
	name:    "purge_alerts_on_deleted_instances",
	fn:      taskPurgeAlertsOnDeletedInstances,
	timeout: time.Minute,
}

var TaskPurgeAlertOnDeletedNodes = Task{
	name:    "purge_alerts_on_deleted_nodes",
	fn:      taskAlertOnDeletedNodes,
	timeout: time.Minute,
}

var TaskPurgeAlertOnDeletedServices = Task{
	name:    "purge_alerts_on_deleted_services",
	fn:      taskPurgeAlertOnDeletedServices,
	timeout: time.Minute,
}

var TaskAlertMACDup = Task{
	name:    "alert_mac_dup",
	fn:      taskAlertMACDup,
	timeout: 5 * time.Minute,
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

var TaskLogAppsWithoutResponsible = Task{
	name:    "log_apps_without_responsible",
	fn:      taskLogAppWithoutResponsible,
	timeout: 5 * time.Minute,
}

var TaskLogInstancesNotUpdated = Task{
	name:    "log_instances_not_updated",
	fn:      taskLogInstancesNotUpdated,
	timeout: 5 * time.Minute,
}

var TaskAlertPackageDifferencesInCluster = Task{
	name:    "alert_package_differences_in_cluster",
	fn:      taskAlertPackageDifferencesInCluster,
	timeout: 5 * time.Minute,
}

var TaskAlertPurgeActionErrors = Task{
	name:    "alert_purge_action_errors",
	fn:      taskAlertPurgeActionErrors,
	timeout: 5 * time.Minute,
}

var TaskAlertUpdateActionErrors = Task{
	name:    "alert_update_action_errors",
	fn:      taskAlertUpdateActionErrors,
	timeout: 5 * time.Minute,
}

var TaskAlertActionErrorsNotAcked = Task{
	name:    "alert_action_errors_not_acked",
	fn:      taskAlertActionErrorsNotAcked,
	timeout: 5 * time.Minute,
}

var TaskAlertServiceConfigNotUpdated = Task{
	name:    "alert_service_config_not_updated",
	fn:      taskAlertServiceConfigNotUpdated,
	timeout: 5 * time.Minute,
}

var TaskAlertInstancesNotUpdated = Task{
	name:    "alert_instances_not_updated",
	fn:      taskAlertInstancesNotUpdated,
	timeout: 5 * time.Minute,
}

var TaskAlertChecksNotUpdated = Task{
	name:    "alert_checks_not_updated",
	fn:      taskAlertChecksNotUpdated,
	timeout: 5 * time.Minute,
}

func taskAlertMACDup(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()
	if err := odb.AlertMACDup(ctx); err != nil {
		return err
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
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

func taskLogAppWithoutResponsible(ctx context.Context, task *Task) error {
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

func taskLogInstancesNotUpdated(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()
	if err := odb.LogInstancesNotUpdated(ctx); err != nil {
		return err
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}

func taskAlertPackageDifferencesInCluster(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()
	if err := odb.DashboardUpdatePkgDiff(ctx); err != nil {
		return err
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}

func taskAlertPurgeActionErrors(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()
	if err := odb.DashboardDeleteActionErrors(ctx); err != nil {
		return err
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}

func taskAlertUpdateActionErrors(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()
	lines, err := odb.GetBActionErrors(ctx)
	if err != nil {
		return err
	}
	for _, line := range lines {
		if err := odb.AlertActionErrors(ctx, line); err != nil {
			return err
		}
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}

func taskAlertActionErrorsNotAcked(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()
	ids, err := odb.GetActionErrorsNotAcked(ctx)
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	if err := odb.LogActionErrorsNotAcked(ctx, ids); err != nil {
		return err
	}
	if err := odb.AutoAckActionErrors(ctx, ids); err != nil {
		return err
	}
	lines, err := odb.GetBActionErrors(ctx)
	if err != nil {
		return err
	}
	for _, line := range lines {
		if err := odb.AlertActionErrors(ctx, line); err != nil {
			return err
		}
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}

func taskAlertOnDeletedNodes(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()

	if err := odb.PurgeAlertsOnDeletedNodes(ctx); err != nil {
		return err
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}

func taskPurgeAlertOnDeletedServices(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()

	if err := odb.PurgeAlertsOnDeletedServices(ctx); err != nil {
		return err
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}

func taskAlertNodesNotUpdated(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()

	if err := odb.DashboardUpdateNodesNotUpdated(ctx); err != nil {
		return err
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}

func taskAlertActionErrorCleanup(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()

	if err := odb.DashboardDeleteActionErrorsWithNoError(ctx); err != nil {
		return err
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}

func taskAlertServiceConfigNotUpdated(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()

	if err := odb.DashboardUpdateServiceConfigNotUpdated(ctx); err != nil {
		return err
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}

func taskAlertInstancesNotUpdated(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()

	if err := odb.DashboardUpdateInstancesNotUpdated(ctx); err != nil {
		return err
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}

func taskAlertNodeMaintenanceExpired(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()

	if err := odb.DashboardUpdateNodeMaintenanceExpired(ctx); err != nil {
		return err
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}

func taskAlertNodeCloseToMaintenanceEnd(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()

	if err := odb.DashboardUpdateNodeCloseToMaintenanceEnd(ctx); err != nil {
		return err
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}

func taskAlertNodeWithoutMaintenanceEnd(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()

	if err := odb.DashboardUpdateNodeWithoutMaintenanceEnd(ctx); err != nil {
		return err
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}

func taskAlertChecksNotUpdated(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()

	if err := odb.DashboardUpdateChecksNotUpdated(ctx); err != nil {
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

	if err := odb.DashboardUpdateAppWithoutResponsible(ctx); err != nil {
		return err
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}

func taskPurgeAlertsOnDeletedInstances(ctx context.Context, task *Task) error {
	odb, err := task.DBX(ctx)
	if err != nil {
		return err
	}
	defer odb.Rollback()

	if err := odb.PurgeAlertsOnDeletedInstances(ctx); err != nil {
		return err
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		return err
	}
	return odb.Commit()
}
