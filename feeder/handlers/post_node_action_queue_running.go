package feederhandlers

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/feeder"
	"github.com/opensvc/oc3/util/logkey"
)

func (a *Api) PostNodeActionQueuedRunning(c echo.Context) error {
	nodeID, clusterID, log := getNodeIDClusterIDAndLogger(c, "PostNodeActionQueuedRunning")

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
	postData := &feeder.QueuedActionRunning{}
	if err := json.Unmarshal(b, postData); err != nil {
		log.Debug("request Unmarshal", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	ctx := c.Request().Context()
	if err := a.ODB.ActionQSetRunningForClusterID(ctx, clusterID, postData.Ids...); err != nil {
		log.Error("set actionq running", logkey.Error, err)
		return JSONProblem(c, http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusAccepted, nil)
}
