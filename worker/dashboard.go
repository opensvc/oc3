package worker

import (
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

func newDashboardObjectUpdate(o *cdb.DBObject, d dashboarder) *cdb.Dashboard {
	now := time.Now()
	return &cdb.Dashboard{
		ObjectID: o.SvcID,
		Type:     d.Type(),
		Fmt:      d.Fmt(),
		Dict:     d.Dict(),
		Env:      o.Env,
		Severity: d.Severity(),
		Created:  now,
		Updated:  now,
	}
}
