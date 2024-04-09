package worker

import (
	"fmt"
)

type (
	DashboardObjectUnavailable struct {
		obj *DBObject
	}
)

func NewDashboardObjectUnavailable(o *DBObject) dashboarder {
	return &DashboardObjectUnavailable{obj: o}
}

func (d *DashboardObjectUnavailable) Type() string {
	return "service unavailable"
}

func (d *DashboardObjectUnavailable) Fmt() string {
	return fmt.Sprintf("current availability status: %s", d.obj.availStatus)
}

func (d *DashboardObjectUnavailable) Dict() string {
	return fmt.Sprintf("{\"s\": \"%s\"}", d.obj.availStatus)
}

func (d *DashboardObjectUnavailable) Severity() int {
	switch d.obj.env {
	case "PRD":
		return 4
	default:
		return 3
	}
}
