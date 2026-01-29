package feederhandlers

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/go-redis/redis/v8"
	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cachekeys"
	"github.com/opensvc/oc3/feeder"
)

func (a *Api) PostFeedDaemonStatus(c echo.Context) error {
	log := getLog(c).With("handler", "PostFeedDaemonStatus")
	nodeID := nodeIDFromContext(c)
	if nodeID == "" {
		log.Debug("node auth problem")
		return JSONNodeAuthProblem(c)
	}

	mChange := make(map[string]struct{})

	mergeChanges := func(s string) {
		for _, v := range strings.Fields(s) {
			mChange[v] = struct{}{}
		}
	}

	body := c.Request().Body
	b, err := io.ReadAll(body)
	defer func() {
		if err := body.Close(); err != nil {
			log.Error("close body failed: " + err.Error())
		}
	}()
	if err != nil {
		return JSONProblemf(c, http.StatusInternalServerError, "", "read request body: %s", err)
	}
	postData := &feeder.PostFeedDaemonStatus{}
	if err := json.Unmarshal(b, postData); err != nil {
		log.Error(fmt.Sprintf("Unmarshal %s", err))
		return JSONProblemf(c, http.StatusBadRequest, "BadRequest", "unexpected body: %s", err)
	} else {
		mergeChanges(strings.Join(postData.Changes, " "))
	}
	if !strings.HasPrefix(postData.Version, "2.") && !strings.HasPrefix(postData.Version, "3.") {
		log.Error(fmt.Sprintf("unexpected version %s", postData.Version))
		return JSONProblemf(c, http.StatusBadRequest, "BadRequest", "unsupported data client version: %s", postData.Version)
	}
	ctx := c.Request().Context()
	log.Info(fmt.Sprintf("HSET %s %s", cachekeys.FeedDaemonStatusH, nodeID))
	if err := a.Redis.HSet(ctx, cachekeys.FeedDaemonStatusH, nodeID, string(b)).Err(); err != nil {
		log.Error(fmt.Sprintf("HSET %s %s", cachekeys.FeedDaemonStatusH, nodeID))
		return JSONProblemf(c, http.StatusInternalServerError, "redis operation", "can't HSET %s: %s", cachekeys.FeedDaemonStatusH, err)
	}
	if len(mChange) > 0 {
		// request contains changes, merge them to not yet applied changes
		log.Info(fmt.Sprintf("HSET %s %s", cachekeys.FeedDaemonStatusChangesH, nodeID))
		redisChanges, err := a.Redis.HGet(ctx, cachekeys.FeedDaemonStatusChangesH, nodeID).Result()
		switch err {
		case nil:
			// merge existing changes
			mergeChanges(redisChanges)
		case redis.Nil:
			// no existing changes to merge
		default:
			return JSONProblemf(c, http.StatusInternalServerError, "redis operation", "can't HGet %s: %s", cachekeys.FeedDaemonStatusChangesH, err)
		}
		l := make([]string, len(mChange))
		i := 0
		for k := range mChange {
			l[i] = k
			i++
		}
		mergedChanges := strings.Join(l, " ")
		// push changes
		log.Info(fmt.Sprintf("HSET %s[%s] with %s", cachekeys.FeedDaemonStatusChangesH, nodeID, mergedChanges))
		if err := a.Redis.HSet(ctx, cachekeys.FeedDaemonStatusChangesH, nodeID, mergedChanges).Err(); err != nil {
			return JSONProblemf(c, http.StatusInternalServerError, "redis operation", "can't HSET %s: %s", cachekeys.FeedDaemonStatusH, err)
		}
	}
	if err := a.pushNotPending(ctx, cachekeys.FeedDaemonStatusPendingH, cachekeys.FeedDaemonStatusQ, nodeID); err != nil {
		log.Error(fmt.Sprintf("can't push %s %s: %s", cachekeys.FeedDaemonStatusQ, nodeID, err))
		return JSONProblemf(c, http.StatusInternalServerError, "redis operation", "can't push %s %s: %s", cachekeys.FeedDaemonStatusQ, nodeID, err)
	}

	clusterID := clusterIDFromContext(c)
	if clusterID != "" {
		objects, err := a.getObjectConfigToFeed(ctx, clusterID)
		if err != nil {
			log.Error(fmt.Sprintf("%s", err))
		} else {
			if len(objects) > 0 {
				if err := a.removeObjectConfigToFeed(ctx, clusterID); err != nil {
					log.Error(fmt.Sprintf("%s", err))
				}
				log.Info(fmt.Sprintf("accepted %s, cluster id %s need object config: %s", nodeID, clusterID, objects))
				return c.JSON(http.StatusAccepted, feeder.FeedDaemonStatusAccepted{ObjectWithoutConfig: &objects})
			}
		}
	}
	return c.JSON(http.StatusAccepted, feeder.FeedDaemonStatusAccepted{})
}
