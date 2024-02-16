package handlers

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cache"
)

func (a *Api) PostDaemonSystemPackage(c echo.Context) error {
	log := getLog(c)
	nodeID := nodeIDFromContext(c)
	if nodeID == "" {
		log.Debug("node auth problem")
		return JSONNodeAuthProblem(c)
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

	reqCtx := c.Request().Context()

	s := fmt.Sprintf("HSET %s %s", cache.KeyPackagesHash, nodeID)
	slog.Info(s)
	if _, err := a.Redis.HSet(reqCtx, cache.KeyPackagesHash, nodeID, string(b)).Result(); err != nil {
		s = fmt.Sprintf("%s: %s", s, err)
		slog.Error(s)
		return JSONProblemf(c, http.StatusInternalServerError, "redis operation", "can't HSET %s: %s", cache.KeyPackagesHash, err)
	}

	if err := a.pushNotPending(reqCtx, cache.KeyPackagesPending, cache.KeyPackages, nodeID); err != nil {
		log.Error(fmt.Sprintf("can't push %s %s: %s", cache.KeyPackages, nodeID, err))
		return JSONProblemf(c, http.StatusInternalServerError, "redis operation", "can't push %s %s: %s", cache.KeyPackages, nodeID, err)
	}
	return c.JSON(http.StatusOK, nil)
}
