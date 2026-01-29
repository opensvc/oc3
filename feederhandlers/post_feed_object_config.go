package feederhandlers

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cachekeys"
	"github.com/opensvc/oc3/feeder"
)

// PostFeedObjectConfig will populate FeedObjectConfigH <path>@<nodeID>@<clusterID>
// with posted object config. The auth middleware has prepared nodeID and clusterID.
func (f *Feeder) PostFeedObjectConfig(c echo.Context) error {
	keyH := cachekeys.FeedObjectConfigH
	keyQ := cachekeys.FeedObjectConfigQ
	keyPendingH := cachekeys.FeedObjectConfigPendingH
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
	var payload feeder.PostFeedObjectConfigJSONRequestBody
	if err := c.Bind(&payload); err != nil {
		return JSONProblem(c, http.StatusBadRequest, "Failed to json decode request body", err.Error())
	}
	if payload.Path == "" {
		log.Debug("bad request: empty path")
		return JSONProblem(c, http.StatusBadRequest, "Got object empty path", "")
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return JSONProblem(c, http.StatusInternalServerError, "Failed to re-encode config", err.Error())
	}
	ctx := c.Request().Context()
	idx := payload.Path + "@" + nodeID + "@" + clusterID
	log.Info(fmt.Sprintf("HSET %s %s", keyH, idx))
	if err := f.Redis.HSet(ctx, keyH, idx, b).Err(); err != nil {
		log.Error(fmt.Sprintf("HSET %s %s", keyH, idx))
		return JSONProblemf(c, http.StatusInternalServerError, "redis operation", "can't HSET %s %s ...: %s", keyH, idx, err)
	}

	if err := f.pushNotPending(ctx, keyPendingH, keyQ, idx); err != nil {
		log.Error(fmt.Sprintf("can't push %s %s: %s", keyQ, idx, err))
		return JSONProblemf(c, http.StatusInternalServerError, "redis operation", "can't push %s %s: %s", keyQ, idx, err)
	}
	return c.JSON(http.StatusAccepted, nil)
}
