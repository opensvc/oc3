package handlers

import (
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/api"
)

func (a *Api) PostDaemonStatus(ctx echo.Context, nodename api.InPathNodeName) error {
	return JSONProblem(ctx, http.StatusInternalServerError, "not yet implemented", "")
}
