package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/opensvc/oc3/cdb"
)

var TaskAlertHourly = Task{
	name: "alerts_hourly",
	children: TaskList{
		TaskAlertNetworkWithWrongMask,
	},
	period:  time.Hour,
	timeout: 15 * time.Minute,
}

var TaskAlertNetworkWithWrongMask = Task{
	name:    "alert_network_with_wrong_mask",
	fn:      taskAlertNetworkWithWrongMask,
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
