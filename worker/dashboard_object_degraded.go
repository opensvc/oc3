package worker

import (
	"fmt"
)

type (
	DashboardObjectDegraded struct {
		obj *DBObject
	}
)

func NewDashboardObjectDegraded(o *DBObject) dashboarder {
	return &DashboardObjectDegraded{obj: o}
}

func (d *DashboardObjectDegraded) Type() string {
	return "service available but degraded"
}

func (d *DashboardObjectDegraded) Fmt() string {
	return fmt.Sprintf("current overall status: %s", d.obj.overallStatus)
}

func (d *DashboardObjectDegraded) Dict() string {
	return fmt.Sprintf("{\"s\": \"%s\"}", d.obj.overallStatus)
}

func (d *DashboardObjectDegraded) Severity() int {
	switch d.obj.env {
	case "PRD":
		return 3
	default:
		return 2
	}
}
