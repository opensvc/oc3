package apihandlers

import (
	"fmt"
	"io"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cache"
)

func (a *Api) PostFeedSystem(c echo.Context) error {
	log := getLog(c)
	nodeID := nodeIDFromContext(c)
	if nodeID == "" {
		return JSONNodeAuthProblem(c)
	}

	b, err := io.ReadAll(c.Request().Body)
	if err != nil {
		return JSONProblemf(c, http.StatusInternalServerError, "", "read request body: %s", err)
	}

	reqCtx := c.Request().Context()

	s := fmt.Sprintf("HSET %s %s", cache.KeyDaemonSystemHash, nodeID)
	log.Info(s)
	if _, err := a.Redis.HSet(reqCtx, cache.KeyDaemonSystemHash, nodeID, string(b)).Result(); err != nil {
		s = fmt.Sprintf("%s: %s", s, err)
		log.Error(s)
		return JSONProblem(c, http.StatusInternalServerError, "", s)
	}

	if err := a.pushNotPending(reqCtx, cache.KeyDaemonSystemPending, cache.KeyDaemonSystem, nodeID); err != nil {
		log.Error(fmt.Sprintf("can't push %s %s: %s", cache.KeyDaemonSystem, nodeID, err))
		return JSONProblemf(c, http.StatusInternalServerError, "redis operation", "can't push %s %s: %s", cache.KeyDaemonSystem, nodeID, err)
	}

	return c.JSON(http.StatusAccepted, nil)
}
