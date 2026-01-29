package feederhandlers

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

func (f *Feeder) GetSwagger(ctx echo.Context) error {
	if !f.UI {
		return JSONProblem(ctx, http.StatusUnauthorized, "serve schema is disabled by configuration.", "listener_feeder.ui.enable = false")
	}
	return ctx.JSON(http.StatusOK, SCHEMA)
}
