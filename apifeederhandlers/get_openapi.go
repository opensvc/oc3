package apifeederhandlers

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

func (a *Api) GetSwagger(ctx echo.Context) error {
	if !a.UI {
		return JSONProblem(ctx, http.StatusUnauthorized, "serve schema is disabled by configuration.", "listener_feed.ui.enable = false")
	}
	return ctx.JSON(http.StatusOK, SCHEMA)
}
