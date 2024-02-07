package handlers

import (
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/api"
)

func (a *Api) PostDaemonStatus(ctx echo.Context, nodename api.InPathNodeName) error {
	nodeID := nodeIDFromContext(ctx)
	if nodeID == "" {
		return JSONProblem(ctx, http.StatusForbidden, "forbidden", "missing node authentication")
	}
	return JSONProblem(ctx, http.StatusInternalServerError, "not yet implemented", "")
}
