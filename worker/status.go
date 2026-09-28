package worker

import "strings"

// status is a copy of the opensvc v3 status type and merge rules from
// github.com/opensvc/om3 core/status/status.go.
// It is used to compute the instance status groups from the resource status.
type status int

const (
	statusUndef status = 0
)

const (
	statusNotApplicable status = 1 << iota
	statusUp
	statusDown
	statusWarn
	statusStandbyUp
	statusStandbyDown
	statusStandbyUpWithUp
	statusStandbyUpWithDown
)

var statusToString = map[status]string{
	statusUp:                "up",
	statusDown:              "down",
	statusWarn:              "warn",
	statusNotApplicable:     "n/a",
	statusUndef:             "undef",
	statusStandbyUp:         "stdby up",
	statusStandbyDown:       "stdby down",
	statusStandbyUpWithUp:   "up",
	statusStandbyUpWithDown: "stdby up",
}

var statusToID = map[string]status{
	"up":         statusUp,
	"down":       statusDown,
	"warn":       statusWarn,
	"n/a":        statusNotApplicable,
	"undef":      statusUndef,
	"stdby up":   statusStandbyUp,
	"stdby down": statusStandbyDown,
}

func parseStatus(s string) status {
	if st, ok := statusToID[strings.TrimSpace(s)]; ok {
		return st
	}
	return statusUndef
}

func (t status) String() string {
	return statusToString[t]
}

// Add merges two states and returns the aggregate state.
func (t *status) Add(o status) {
	// handle invariants
	if o == statusUndef {
		return
	}
	if *t == statusUndef {
		*t = o
		return
	}
	if o == statusNotApplicable {
		return
	}
	if *t == statusNotApplicable {
		*t = o
		return
	}

	// other merges
	switch *t | o {
	case statusUp | statusUp:
		*t = statusUp
	case statusUp | statusDown:
		*t = statusWarn
	case statusUp | statusWarn:
		*t = statusWarn
	case statusUp | statusStandbyUp:
		*t = statusStandbyUpWithUp
	case statusUp | statusStandbyDown:
		*t = statusWarn
	case statusUp | statusStandbyUpWithUp:
		*t = statusStandbyUpWithUp
	case statusUp | statusStandbyUpWithDown:
		*t = statusWarn
	case statusDown | statusDown:
		*t = statusDown
	case statusDown | statusWarn:
		*t = statusWarn
	case statusDown | statusStandbyUp:
		*t = statusStandbyUpWithDown
	case statusDown | statusStandbyDown:
		*t = statusStandbyDown
	case statusDown | statusStandbyUpWithUp:
		*t = statusWarn
	case statusDown | statusStandbyUpWithDown:
		*t = statusStandbyUpWithDown
	case statusWarn | statusWarn:
		*t = statusWarn
	case statusWarn | statusStandbyUp:
		*t = statusWarn
	case statusWarn | statusStandbyDown:
		*t = statusWarn
	case statusWarn | statusStandbyUpWithUp:
		*t = statusWarn
	case statusWarn | statusStandbyUpWithDown:
		*t = statusWarn
	case statusStandbyUp | statusStandbyUp:
		*t = statusStandbyUp
	case statusStandbyUp | statusStandbyDown:
		*t = statusWarn
	case statusStandbyUp | statusStandbyUpWithUp:
		*t = statusStandbyUpWithUp
	case statusStandbyUp | statusStandbyUpWithDown:
		*t = statusStandbyUpWithDown
	case statusStandbyDown | statusStandbyDown:
		*t = statusStandbyDown
	case statusStandbyDown | statusStandbyUpWithUp:
		*t = statusWarn
	case statusStandbyDown | statusStandbyUpWithDown:
		*t = statusWarn
	case statusStandbyUpWithUp | statusStandbyUpWithDown:
		*t = statusWarn
	case statusStandbyUpWithUp | statusStandbyUpWithUp:
		*t = statusStandbyUpWithUp
	case statusStandbyUpWithDown | statusStandbyUpWithDown:
		*t = statusStandbyUpWithDown
	}
}
