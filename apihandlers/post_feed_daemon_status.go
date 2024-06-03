package apihandlers

import (
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/go-redis/redis/v8"
	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/api"
	"github.com/opensvc/oc3/cachekeys"
)

func (a *Api) PostFeedDaemonStatus(c echo.Context, params api.PostFeedDaemonStatusParams) error {
	log := getLog(c)
	nodeID := nodeIDFromContext(c)
	if nodeID == "" {
		log.Debug("node auth problem")
		return JSONNodeAuthProblem(c)
	}

	mChange := make(map[string]struct{})

	mergeChanges := func(s string) error {
		for _, v := range strings.Fields(s) {
			mChange[v] = struct{}{}
		}
		return nil
	}

	if params.XDaemonChange != nil {
		changes := *params.XDaemonChange
		if err := mergeChanges(changes); err != nil {
			return JSONProblemf(c, http.StatusBadRequest, "BadRequest", "unexpected changes %s: %s", changes, err)
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
			if err := mergeChanges(redisChanges); err != nil {
				log.Warn(fmt.Sprintf("ignore invalid value %s %s", cachekeys.FeedDaemonStatusChangesH, nodeID))
			}
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
	return c.JSON(http.StatusAccepted, nil)
}
