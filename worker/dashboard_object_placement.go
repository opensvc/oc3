package worker

import "fmt"

type (
	DashboardObjectPlacement struct {
		obj *DBObject
	}
)

func (d *DashboardObjectPlacement) Type() string {
	return "service placement"
}

func (d *DashboardObjectPlacement) Fmt() string {
	return "%(placement)s"
}

func (d *DashboardObjectPlacement) Dict() string {
	return fmt.Sprintf("{\"placement\": \"%s\"}", d.obj.placement)
}

func (d *DashboardObjectPlacement) Severity() int {
	return severityFromEnv(dashObjObjectPlacement, d.obj.env)
}
