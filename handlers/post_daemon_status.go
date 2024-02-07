package handlers

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

func (a *Api) PostDaemonStatus(ctx echo.Context) error {
	nodeID := nodeIDFromContext(ctx)
	if nodeID == "" {
		return JSONProblem(ctx, http.StatusForbidden, "forbidden", "missing node authentication")
	}
	return JSONProblem(ctx, http.StatusInternalServerError, "not yet implemented", "")
}
