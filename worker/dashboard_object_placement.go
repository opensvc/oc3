package worker

import (
	"fmt"

	"github.com/opensvc/oc3/cdb"
)

type (
	DashboardObjectPlacement struct {
		obj *cdb.DBObject
	}
)

func (d *DashboardObjectPlacement) Type() string {
	return "service placement"
}

func (d *DashboardObjectPlacement) Fmt() string {
	return "%(placement)s"
}

func (d *DashboardObjectPlacement) Dict() string {
	return fmt.Sprintf("{\"placement\": \"%s\"}", d.obj.Placement)
}

func (d *DashboardObjectPlacement) Severity() int {
	return severityFromEnv(dashObjObjectPlacement, d.obj.Env)
}
