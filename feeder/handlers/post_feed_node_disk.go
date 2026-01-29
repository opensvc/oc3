package feederhandlers

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cachekeys"
	"github.com/opensvc/oc3/feeder"
)

// PostNodeDisk will populate FeedNodeDiskH <nodename>@<nodeID>@<clusterID>
// with posted disk, auth middleware has prepared nodeID, clusterID, and nodename.
func (a *Api) PostNodeDisk(c echo.Context) error {
	keyH := cachekeys.FeedNodeDiskH
	keyQ := cachekeys.FeedNodeDiskQ
	keyPendingH := cachekeys.FeedNodeDiskPendingH
	log := getLog(c)
	nodeID := nodeIDFromContext(c)
	if nodeID == "" {
		log.Debug("node auth problem")
		return JSONNodeAuthProblem(c)
	}
	nodename := nodenameFromContext(c)
	if nodename == "" {
		return JSONProblemf(c, http.StatusConflict, "Refused", "authenticated node doesn't define nodename")
	}
	clusterID := clusterIDFromContext(c)
	if clusterID == "" {
		return JSONProblemf(c, http.StatusConflict, "Refused", "authenticated node doesn't define cluster id")
	}
	var payload feeder.PostNodeDiskJSONRequestBody
	if err := c.Bind(&payload); err != nil {
		return JSONProblem(c, http.StatusBadRequest, "Failed to json decode request body", err.Error())
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return JSONProblem(c, http.StatusInternalServerError, "Failed to re-encode config", err.Error())
	}
	ctx := c.Request().Context()
	idx := nodename + "@" + nodeID + "@" + clusterID
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
