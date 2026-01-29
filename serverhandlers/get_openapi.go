package serverhandlers

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

func (s *Server) GetSwagger(ctx echo.Context) error {
	if !s.UI {
		return JSONProblem(ctx, http.StatusUnauthorized, "serve schema is disabled by configuration.", "listener_server.ui.enable = false")
	}
	return ctx.JSON(http.StatusOK, SCHEMA)
}
