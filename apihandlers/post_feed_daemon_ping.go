package apihandlers

import (
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/api"
	"github.com/opensvc/oc3/cachekeys"
)

func (a *Api) PostFeedDaemonPing(c echo.Context) error {
	log := getLog(c).With("handler", "PostFeedDaemonPing")
	nodeID := nodeIDFromContext(c)
	if nodeID == "" {
		log.Debug("node auth problem")
		return JSONNodeAuthProblem(c)
	}

	ctx := c.Request().Context()
	if hasStatus, err := a.Redis.HExists(ctx, cachekeys.FeedDaemonStatusH, nodeID).Result(); err != nil {
		return JSONProblemf(c, http.StatusInternalServerError, "redis operation", "can't HGet %s: %s", cachekeys.FeedDaemonStatusChangesH, err)
	} else if !hasStatus {
		// no daemon status for node. We need first POST /daemon/status
		log.Debug(fmt.Sprintf("need resync %s", nodeID))
		return c.NoContent(http.StatusNoContent)
	}

	if err := a.pushNotPending(ctx, cachekeys.FeedDaemonPingPendingH, cachekeys.FeedDaemonPingQ, nodeID); err != nil {
		log.Error(fmt.Sprintf("can't push %s %s: %s", cachekeys.FeedDaemonPingQ, nodeID, err))
		return JSONProblemf(c, http.StatusInternalServerError, "redis operation", "can't push %s %s: %s", cachekeys.FeedDaemonPingQ, nodeID, err)
	}
	clusterID := clusterIDFromContext(c)

	if clusterID != "" {
		objects, err := a.getObjectConfigToFeed(ctx, clusterID)
		if err != nil {
			log.Error("%s", err)
		} else {
			if len(objects) > 0 {
				if err := a.removeObjectConfigToFeed(ctx, clusterID); err != nil {
					log.Error("%s", err)
				}
				log.Info(fmt.Sprintf("accepted %s, cluster id %s need object config: %s", nodeID, clusterID, objects))
				return c.JSON(http.StatusAccepted, api.FeedDaemonPingAccepted{ObjectWithoutConfig: &objects})
			}
		}
	}
	log.Info(fmt.Sprintf("accepted %s", nodeID))
	return c.JSON(http.StatusAccepted, api.FeedDaemonPingAccepted{})
}
