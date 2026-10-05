package feederhandlers

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cachekeys"
	"github.com/opensvc/oc3/feeder"
	"github.com/opensvc/oc3/util/logkey"
)

// PostNodeChecks will populate FeedNodeChecksH <nodename>@<nodeID>@<clusterID>
// with the posted checks, auth middleware has prepared nodeID, clusterID, and
// nodename. The cluster id resolves the object paths of the checks.
func (a *Api) PostNodeChecks(c echo.Context) error {
	nodeID, log := getNodeIDAndLogger(c, "PostNodeChecks")
	if nodeID == "" {
		return JSONNodeAuthProblem(c)
	}

	keyH := cachekeys.FeedNodeChecksH
	keyQ := cachekeys.FeedNodeChecksQ
	keyPendingH := cachekeys.FeedNodeChecksPendingH

	nodename := nodenameFromContext(c)
	if nodename == "" {
		return JSONProblemf(c, http.StatusConflict, "refused: authenticated node doesn't define nodename")
	}
	clusterID := clusterIDFromContext(c)
	if clusterID == "" {
		return JSONProblemf(c, http.StatusConflict, "refused: authenticated node doesn't define cluster id")
	}
	var payload feeder.PostNodeChecksJSONRequestBody
	if err := c.Bind(&payload); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	b, err := json.Marshal(payload)
	if err != nil {
		log.Warn("Marshal", logkey.Error, err)
		return JSONError(c)
	}
	ctx := c.Request().Context()
	idx := nodename + "@" + nodeID + "@" + clusterID
	if err := a.Redis.HSet(ctx, keyH, idx, b).Err(); err != nil {
		log.Error("HSet keyH", logkey.Error, err)
		return JSONError(c)
	}

	if err := a.pushNotPending(ctx, log, keyPendingH, keyQ, idx); err != nil {
		log.Error("pushNotPending", logkey.Error, err)
		return JSONError(c)
	}
	return c.JSON(http.StatusAccepted, nil)
}
