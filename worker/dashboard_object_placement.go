package worker

import "fmt"

type (
	DashboardObjectPlacement struct {
		obj *DBObject
	}
)

func NewDashboardObjectPlacement(o *DBObject) dashboarder {
	return &DashboardObjectPlacement{obj: o}
}

func (d *DashboardObjectPlacement) Type() string {
	return "service placement"
}

func (d *DashboardObjectPlacement) Fmt() string {
	return fmt.Sprintf("%s", d.obj.placement)
}

func (d *DashboardObjectPlacement) Dict() string {
	return fmt.Sprintf("{\"placement\": \"%s\"}", d.obj.placement)
}

func (d *DashboardObjectPlacement) Severity() int {
	return 1
}
