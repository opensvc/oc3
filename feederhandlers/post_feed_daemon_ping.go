package feederhandlers

import (
	"fmt"
	"io"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cachekeys"
	"github.com/opensvc/oc3/feeder"
)

func (f *Feeder) PostFeedDaemonPing(c echo.Context) error {
	log := getLog(c).With("handler", "PostFeedDaemonPing")
	nodeID := nodeIDFromContext(c)
	if nodeID == "" {
		log.Debug("node auth problem")
		return JSONNodeAuthProblem(c)
	}

	ctx := c.Request().Context()
	if hasStatus, err := f.Redis.HExists(ctx, cachekeys.FeedDaemonStatusH, nodeID).Result(); err != nil {
		return JSONProblemf(c, http.StatusInternalServerError, "redis operation", "can't HGet %s: %s", cachekeys.FeedDaemonStatusChangesH, err)
	} else if !hasStatus {
		// no daemon status for node. We need first POST /daemon/status
		log.Debug(fmt.Sprintf("need resync %s", nodeID))
		return c.NoContent(http.StatusNoContent)
	}

	body := c.Request().Body
	b, err := io.ReadAll(body)
	defer func() {
		if err := body.Close(); err != nil {
			log.Error("close body failed: " + err.Error())
		}
	}()
	if err := f.Redis.HSet(ctx, cachekeys.FeedDaemonPingH, nodeID, string(b)).Err(); err != nil {
		log.Error(fmt.Sprintf("HSET %s %s", cachekeys.FeedDaemonPingH, nodeID))
		return JSONProblemf(c, http.StatusInternalServerError, "redis operation", "can't HSET %s: %s", cachekeys.FeedDaemonPingH, err)
	}

	if err != nil {
		return JSONProblemf(c, http.StatusInternalServerError, "", "read request body: %s", err)
	}
	if err := f.pushNotPending(ctx, cachekeys.FeedDaemonPingPendingH, cachekeys.FeedDaemonPingQ, nodeID); err != nil {
		log.Error(fmt.Sprintf("can't push %s %s: %s", cachekeys.FeedDaemonPingQ, nodeID, err))
		return JSONProblemf(c, http.StatusInternalServerError, "redis operation", "can't push %s %s: %s", cachekeys.FeedDaemonPingQ, nodeID, err)
	}
	clusterID := clusterIDFromContext(c)

	if clusterID != "" {
		objects, err := f.getObjectConfigToFeed(ctx, clusterID)
		if err != nil {
			log.Error(fmt.Sprintf("%s", err))
		} else {
			if len(objects) > 0 {
				if err := f.removeObjectConfigToFeed(ctx, clusterID); err != nil {
					log.Error(fmt.Sprintf("%s", err))
				}
				log.Info(fmt.Sprintf("accepted %s, cluster id %s need object config: %s", nodeID, clusterID, objects))
				return c.JSON(http.StatusAccepted, feeder.FeedDaemonPingAccepted{ObjectWithoutConfig: &objects})
			}
		}
	}
	log.Info(fmt.Sprintf("accepted %s", nodeID))
	return c.JSON(http.StatusAccepted, feeder.FeedDaemonPingAccepted{})
}
