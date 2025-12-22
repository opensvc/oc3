package apihandlers

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/api"
	"github.com/opensvc/oc3/cachekeys"
)

// PostFeedInstanceResourceInfo will populate FeedInstanceResourceInfo <path>@<nodeID>@<clusterID>
// with posted instance resource information. The auth middleware has prepared nodeID and clusterID.
func (a *Api) PostFeedInstanceResourceInfo(c echo.Context) error {
	var data api.PostFeedInstanceResourceInfoJSONRequestBody
	keyH := cachekeys.FeedInstanceResourceInfoH
	keyQ := cachekeys.FeedInstanceResourceInfoQ
	keyPendingH := cachekeys.FeedInstanceResourceInfoPendingH
	log := getLog(c)
	nodeID := nodeIDFromContext(c)
	if nodeID == "" {
		log.Debug("node auth problem")
		return JSONNodeAuthProblem(c)
	}
	clusterID := clusterIDFromContext(c)
	if clusterID == "" {
		return JSONProblemf(c, http.StatusConflict, "Refused", "authenticated node doesn't define cluster id")
	}

	if err := c.Bind(&data); err != nil {
		return JSONProblem(c, http.StatusBadRequest, "Failed to json decode request body", err.Error())
	}
	if data.Path == "" {
		log.Debug("bad request: missing or empty instance path")
		return JSONProblem(c, http.StatusBadRequest, "BadRequest: missing or empty instance path", "")
	}
	b, err := json.Marshal(data)
	if err != nil {
		return JSONProblem(c, http.StatusInternalServerError, "Failed to re-encode config", err.Error())
	}
	ctx := c.Request().Context()
	idx := data.Path + "@" + nodeID + "@" + clusterID
	log.Info(fmt.Sprintf("HSET %s %s", keyH, idx))
	if err := a.Redis.HSet(ctx, keyH, idx, b).Err(); err != nil {
		log.Error(fmt.Sprintf("HSET %s %s", keyH, idx))
		return JSONProblemf(c, http.StatusInternalServerError, "redis operation", "can't HSET %s %s ...: %s", keyH, idx, err)
	}

	if err := a.pushNotPending(ctx, keyPendingH, keyQ, idx); err != nil {
		log.Error(fmt.Sprintf("can't push %s %s: %s", keyQ, idx, err))
		return JSONProblemf(c, http.StatusInternalServerError, "redis operation", "can't push %s %s: %s", keyQ, idx, err)
	}
	return c.JSON(http.StatusAccepted, nil)
}
