package handlers

import (
	"fmt"
	"log/slog"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/api"
)

func (a *Api) PostDaemonStatus(ctx echo.Context, nodename api.InPathNodeName) error {
	if user := getUser(ctx); user != nil {
		slog.Debug(fmt.Sprint("PostDaemonStatus called by user %#v", user.GetUserName()))
	}
	return JSONProblem(ctx, http.StatusInternalServerError, "not yet implemented", "")
}
