package worker

import (
	"fmt"
)

type (
	DashboardObjectUnavailable struct {
		obj *DBObject
	}
)

func (d *DashboardObjectUnavailable) Type() string {
	return "service unavailable"
}

func (d *DashboardObjectUnavailable) Fmt() string {
	return "current availability status: %(s)s"
}

func (d *DashboardObjectUnavailable) Dict() string {
	return fmt.Sprintf("{\"s\": \"%s\"}", d.obj.availStatus)
}

func (d *DashboardObjectUnavailable) Severity() int {
	return severityFromEnv(dashObjObjectUnavailable, d.obj.env)
}
