package worker

import (
	"fmt"
	"time"

	"github.com/opensvc/oc3/cdb"
)

type (
	dashboarder interface {
		Type() string
		Fmt() string
		Dict() string
		Severity() int
	}
)

const (
	dashObjObjectDegraded = iota
	dashObjObjectFlexError
	dashObjObjectPlacement
	dashObjObjectUnavailable
)

var (
	severity = map[int]map[string]int{
		dashObjObjectDegraded:    map[string]int{"DEFAULT": 2, "PRD": 3},
		dashObjObjectFlexError:   map[string]int{"DEFAULT": 5, "PRD": 4},
		dashObjObjectPlacement:   map[string]int{"DEFAULT": 1},
		dashObjObjectUnavailable: map[string]int{"DEFAULT": 3, "PRD": 4},
	}
)

func severityFromEnv(dashType int, objEnv string) int {
	severityForType := severity[dashType]
	if severityForType == nil {
		return 0
	}
	if v, ok := severityForType[objEnv]; ok {
		return v
	} else {
		return severityForType["DEFAULT"]
	}
}

func (d *jobFeedDaemonStatus) updateDashboardObject(obj *cdb.DBObject, doDelete bool, dash dashboarder) error {
	objID := obj.SvcID
	fmtErr := func(err error) error {
		if err != nil {
			return fmt.Errorf("updateDashboardObject '%s': %w", dash.Type(), err)
		}
		return nil
	}

	if doDelete {
		return fmtErr(d.oDb.DashboardDeleteObjectWithType(d.ctx, objID, dash.Type()))
	}

	inAckPeriod, err := d.oDb.ObjectInAckUnavailabilityPeriod(d.ctx, objID)
	if err != nil {
		return err
	}
	dashType := dash.Type()
	if inAckPeriod {
		return fmtErr(d.oDb.DashboardDeleteObjectWithType(d.ctx, objID, dashType))
	} else {
		now := time.Now()
		dash := cdb.Dashboard{
			ObjectID: objID,
			Type:     dashType,
			Fmt:      dash.Fmt(),
			Dict:     dash.Dict(),
			Env:      obj.Env,
			Severity: dash.Severity(),
			Created:  now,
			Updated:  now,
		}
		return fmtErr(d.oDb.DashboardUpdateObject(d.ctx, &dash))
	}
}
