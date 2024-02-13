package handlers

import (
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/go-redis/redis/v8"
	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/api"
	"github.com/opensvc/oc3/cache"
)

func (a *Api) PostDaemonStatus(c echo.Context, params api.PostDaemonStatusParams) error {
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
	log.Info(fmt.Sprintf("HSET %s %s", cache.KeyDaemonStatusHash, nodeID))
	if err := a.Redis.HSet(ctx, cache.KeyDaemonStatusHash, nodeID, string(b)).Err(); err != nil {
		log.Error(fmt.Sprintf("HSET %s %s", cache.KeyDaemonStatusHash, nodeID))
		return JSONProblemf(c, http.StatusInternalServerError, "redis operation", "can't HSET %s: %s", cache.KeyDaemonStatusHash, err)
	}
	if len(mChange) > 0 {
		// request contains changes, merge them to not yet applied changes
		log.Info(fmt.Sprintf("HSET %s %s", cache.KeyDaemonStatusChangesHash, nodeID))
		redisChanges, err := a.Redis.HGet(ctx, cache.KeyDaemonStatusChangesHash, nodeID).Result()
		switch err {
		case nil:
			// merge existing changes
			if err := mergeChanges(redisChanges); err != nil {
				log.Warn(fmt.Sprintf("ignore invalid value %s %s", cache.KeyDaemonStatusChangesHash, nodeID))
			}
		case redis.Nil:
			// no existing changes to merge
		default:
			return JSONProblemf(c, http.StatusInternalServerError, "redis operation", "can't HGet %s: %s", cache.KeyDaemonStatusChangesHash, err)
		}
		l := make([]string, len(mChange))
		i := 0
		for k := range mChange {
			l[i] = k
			i++
		}
		mergedChanges := strings.Join(l, " ")
		// push changes
		log.Info(fmt.Sprintf("HSET %s[%s] with %s", cache.KeyDaemonStatusChangesHash, nodeID, mergedChanges))
		if err := a.Redis.HSet(ctx, cache.KeyDaemonStatusChangesHash, nodeID, mergedChanges).Err(); err != nil {
			return JSONProblemf(c, http.StatusInternalServerError, "redis operation", "can't HSET %s: %s", cache.KeyDaemonStatusHash, err)
		}
	}
	if err := a.pushNotPending(ctx, cache.KeyDaemonStatusPending, cache.KeyDaemonStatus, nodeID); err != nil {
		log.Error(fmt.Sprintf("can't push %s %s: %s", cache.KeyDaemonStatus, nodeID, err))
		return JSONProblemf(c, http.StatusInternalServerError, "redis operation", "can't push %s %s: %s", cache.KeyDaemonStatus, nodeID, err)
	}
	return c.JSON(http.StatusOK, nil)
}
