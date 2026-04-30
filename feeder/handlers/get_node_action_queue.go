package feederhandlers

import (
	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/feeder"
	"github.com/opensvc/oc3/util/logkey"
)

func (a *Api) GetNodeActionQueued(c echo.Context) error {
	nodeID, log := getNodeIDAndLogger(c, "GetNodeActionQueued")
	if nodeID == "" {
		return JSONNodeAuthProblem(c)
	}
	ctx := c.Request().Context()
	var l []cdb.ActionQueueNamedEntry
	l, err := a.ODB.ActionQByNodeID(ctx, nodeID)
	if err != nil {
		log.Error("get node action queued", logkey.Error, err)
		return JSONError(c)
	}

	actions := make([]feeder.QueuedAction, len(l))
	ids := make([]int, len(l))
	for i, a := range l {
		actions[i] = feeder.QueuedAction{
			Command:    a.Command,
			DequeuedAt: a.DateDequeued,
			QueuedAt:   a.DateQueued,
			Id:         a.ID,
			SvcId:      a.SvcId,
			Svcname:    a.Svcname,
			Status:     a.Status,
		}
		if a.ConnectTo != nil {
			actions[i].Node = *a.ConnectTo
		}
		ids[i] = a.ID
	}
	if err := a.ODB.ActionQSetSent(ctx, ids...); err != nil {
		log.Error("set action sent", logkey.Error, err)
		return JSONError(c)
	}
	return c.JSON(200, feeder.QueuedActions{Actions: actions})
}
