package worker

import (
	"fmt"
	"time"
)

type (
	dashboarder interface {
		Type() string
		Fmt() string
		Dict() string
		Severity() int
	}

	dashboarderCreate func(o *DBObject) dashboarder
)

func (d *daemonStatus) updateDashboardObject(obj *DBObject, doDelete bool, f dashboarderCreate) error {
	objID := obj.svcID
	dash := f(obj)
	fmtErr := func(err error) error {
		if err != nil {
			return fmt.Errorf("updateDashboardObject '%s': %w", dash.Type(), err)
		}
		return nil
	}

	if doDelete {
		return fmtErr(d.oDb.dashboardDeleteObjectWithType(d.ctx, objID, dash.Type()))
	}

	inAckPeriod, err := d.oDb.ObjectInAckUnavailabilityPeriod(d.ctx, objID)
	if err != nil {
		return err
	}
	dashType := dash.Type()
	if inAckPeriod {
		return fmtErr(d.oDb.dashboardDeleteObjectWithType(d.ctx, objID, dashType))
	} else {
		now := time.Now()
		dash := Dashboard{
			ObjectID: objID,
			Type:     dashType,
			Fmt:      dash.Fmt(),
			Dict:     dash.Dict(),
			Env:      obj.env,
			Severity: dash.Severity(),
			Created:  now,
			Updated:  now,
		}
		return fmtErr(d.oDb.dashboardUpdateObject(d.ctx, &dash))
	}
}
