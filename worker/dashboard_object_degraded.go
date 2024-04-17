package worker

import (
	"fmt"
)

type (
	DashboardObjectDegraded struct {
		obj *DBObject
	}
)

func (d *DashboardObjectDegraded) Type() string {
	return "service available but degraded"
}

func (d *DashboardObjectDegraded) Fmt() string {
	return "current overall status: %(s)s"
}

func (d *DashboardObjectDegraded) Dict() string {
	return fmt.Sprintf("{\"s\": \"%s\"}", d.obj.overallStatus)
}

func (d *DashboardObjectDegraded) Severity() int {
	return severityFromEnv(dashObjObjectDegraded, d.obj.env)
}
