package feederhandlers

import (
	"fmt"
	"io"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cachekeys"
	"github.com/opensvc/oc3/feeder"
	"github.com/opensvc/oc3/util/logkey"
)

func (a *Api) PostDaemonPing(c echo.Context) error {
	nodeID, clusterID, log := getNodeIDClusterIDAndLogger(c, "PostDaemonPing")
	if nodeID == "" {
		return JSONNodeAuthProblem(c)
	}

	ctx := c.Request().Context()
	if hasStatus, err := a.Redis.HExists(ctx, cachekeys.FeedDaemonStatusH, nodeID).Result(); err != nil {
		log.Error("HExists FeedDaemonStatusH", logkey.Error, err)
		return JSONError(c)
	} else if !hasStatus {
		// still waiting for the initial daemon status post.
		log.Debug("need resync")
		return c.NoContent(http.StatusNoContent)
	}

	body := c.Request().Body
	b, err := io.ReadAll(body)
	defer func() {
		if err := body.Close(); err != nil {
			log.Warn("Close", logkey.Error, err)
		}
	}()
	if err != nil {
		return JSONProblemf(c, http.StatusBadRequest, "ReadAll: %s", err)
	}

	log.Debug("Hset FeedDaemonPingH")
	if err := a.Redis.HSet(ctx, cachekeys.FeedDaemonPingH, nodeID, string(b)).Err(); err != nil {
		log.Error("Hset FeedDaemonPingH", logkey.Error, err)
		return JSONError(c)
	}

	if err := a.pushNotPending(ctx, log, cachekeys.FeedDaemonPingPendingH, cachekeys.FeedDaemonPingQ, nodeID); err != nil {
		log.Error("pushNotPending", logkey.Error, err)
		return JSONError(c)
	}

	responseData := feeder.DaemonPingAccepted{}
	if clusterID != "" {
		if nodes, err := a.getNodeWithActionQueued(ctx, clusterID); err != nil {
			log.Warn("nodesWithActionQueued", logkey.Error, err)
		} else {
			responseData.NodeWithActionQueued = &nodes
			// TODO: add metric
			log.Debug(fmt.Sprintf("found action queued for nodes: %s", nodes))
		}
		if objects, err := a.getObjectConfigToFeed(ctx, clusterID); err != nil {
			log.Warn("getObjectConfigToFeed", logkey.Error, err)
		} else if len(objects) > 0 {
			if err := a.removeObjectConfigToFeed(ctx, clusterID); err != nil {
				log.Warn("removeObjectConfigToFeed", logkey.Error, err)
			}
			responseData.ObjectWithoutConfig = &objects
			// TODO: add metric
			log.Debug("detect missing object configs", logkey.Objects, objects)
		}
	}
	log.Debug("accepted")
	return c.JSON(http.StatusAccepted, responseData)
}
