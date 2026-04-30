package feederhandlers

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/feeder"
	"github.com/opensvc/oc3/util/logkey"
)

func (a *Api) PostNodeActionQueuedDone(c echo.Context) error {
	nodeID, log := getNodeIDAndLogger(c, "PostNodeActionQueuedDone")

	if nodeID == "" {
		return JSONNodeAuthProblem(c)
	}

	body := c.Request().Body
	b, err := io.ReadAll(body)
	defer func() {
		if err := body.Close(); err != nil {
			log.Warn("request body Close", logkey.Error, err)
		}
	}()
	if err != nil {
		log.Warn("request ReadAll", logkey.Error, err)
		return JSONProblemf(c, http.StatusBadRequest, "ReadAll: %s", err)
	}
	data := &feeder.QueuedActionDone{}
	if err := json.Unmarshal(b, data); err != nil {
		log.Debug("request Unmarshal", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	ctx := c.Request().Context()
	action := cdb.ActionQueueNamedEntry{
		ActionQueueEntry: cdb.ActionQueueEntry{
			ID:           data.Id,
			Ret:          data.Ret,
			Stdout:       data.Stdout,
			Stderr:       data.Stderr,
			DateDequeued: data.DequeuedAt,
		},
	}
	if err := a.ODB.ActionQSetDoneForNodeID(ctx, nodeID, action); err != nil {
		log.Error("set received actionq", logkey.Error, err)
		return JSONProblem(c, http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusAccepted, nil)
}
