package scheduler

import (
	"context"
	"time"

	"github.com/opensvc/oc3/availability"
)

// TaskServicesAvailability stores the availability rate of the last 30 days of
// every service, which the Services view sorts and filters by: the rate moves
// as the statuses are received, hence the short period.
var TaskServicesAvailability = Task{
	name:    "services_availability",
	desc:    "store the 30-day availability rate of the services",
	period:  10 * time.Minute,
	fn:      taskServicesAvailability,
	timeout: 5 * time.Minute,
}

func taskServicesAvailability(ctx context.Context, task *Task) error {
	odb := task.DB()
	ids, err := odb.ServiceIDs(ctx)
	if err != nil {
		return err
	}
	return availability.Refresh(ctx, odb, ids, time.Now())
}
