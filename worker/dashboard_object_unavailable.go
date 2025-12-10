package worker

import (
	"fmt"

	"github.com/opensvc/oc3/cdb"
)

type (
	DashboardObjectUnavailable struct {
		obj *cdb.DBObject
	}
)

func (d *DashboardObjectUnavailable) Type() string {
	return "service unavailable"
}

func (d *DashboardObjectUnavailable) Fmt() string {
	return "current availability status: %(s)s"
}

func (d *DashboardObjectUnavailable) Dict() string {
	return fmt.Sprintf("{\"s\": \"%s\"}", d.obj.AvailStatus)
}

func (d *DashboardObjectUnavailable) Severity() int {
	return severityFromEnv(dashObjObjectUnavailable, d.obj.Env)
}
