package apihandlers

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/go-redis/redis/v8"
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
		keyName := cachekeys.FeedObjectConfigForClusterIDH
		if s, err := a.Redis.HGet(ctx, keyName, clusterID).Result(); err != nil {
			if !errors.Is(err, redis.Nil) {
				log.Error(fmt.Sprintf("HGET %s %s: %s", keyName, clusterID, err))
			}
		} else if l := strings.Fields(s); len(l) > 0 {
			log.Debug(fmt.Sprintf("accepted %s, cluster id %s needs objects %#v", nodeID, clusterID, l))
			defer func() {
				// Cleanup, client has been notified
				if err := a.Redis.HDel(ctx, keyName, clusterID).Err(); err != nil {
					log.Error(fmt.Sprintf("HDEL %s %s: %s", keyName, clusterID, err))
				}
			}()
			return c.JSON(http.StatusAccepted, api.FeedDaemonPingAccepted{ObjectWithoutConfig: &l})
		}
	}
	log.Debug(fmt.Sprintf("accepted %s", nodeID))
	return c.JSON(http.StatusAccepted, api.FeedDaemonPingAccepted{})
}
