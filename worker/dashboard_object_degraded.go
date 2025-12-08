package worker

import (
	"fmt"

	"github.com/opensvc/oc3/cdb"
)

type (
	DashboardObjectDegraded struct {
		obj *cdb.DBObject
	}
)

func (d *DashboardObjectDegraded) Type() string {
	return "service available but degraded"
}

func (d *DashboardObjectDegraded) Fmt() string {
	return "current overall status: %(s)s"
}

func (d *DashboardObjectDegraded) Dict() string {
	return fmt.Sprintf("{\"s\": \"%s\"}", d.obj.OverallStatus)
}

func (d *DashboardObjectDegraded) Severity() int {
	return severityFromEnv(dashObjObjectDegraded, d.obj.Env)
}
