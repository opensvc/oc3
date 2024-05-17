package apihandlers

import (
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cache"
)

func (a *Api) PostFeedDaemonPing(c echo.Context) error {
	log := getLog(c).With("handler", "PostFeedDaemonPing")
	nodeID := nodeIDFromContext(c)
	if nodeID == "" {
		log.Debug("node auth problem")
		return JSONNodeAuthProblem(c)
	}

	ctx := c.Request().Context()
	if hasStatus, err := a.Redis.HExists(ctx, cache.KeyDaemonStatusHash, nodeID).Result(); err != nil {
		return JSONProblemf(c, http.StatusInternalServerError, "redis operation", "can't HGet %s: %s", cache.KeyDaemonStatusChangesHash, err)
	} else if !hasStatus {
		// no daemon status for node. We need first POST /daemon/status
		log.Debug(fmt.Sprintf("need resync %s", nodeID))
		return c.NoContent(http.StatusNoContent)
	}

	if err := a.pushNotPending(ctx, cache.KeyDaemonPingPending, cache.KeyDaemonPing, nodeID); err != nil {
		log.Error(fmt.Sprintf("can't push %s %s: %s", cache.KeyDaemonPing, nodeID, err))
		return JSONProblemf(c, http.StatusInternalServerError, "redis operation", "can't push %s %s: %s", cache.KeyDaemonPing, nodeID, err)
	}
	log.Debug(fmt.Sprintf("accepted %s", nodeID))
	return c.JSON(http.StatusAccepted, nil)
}
