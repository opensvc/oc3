package serverhandlers

import (
	"context"
	"net/http"

	"github.com/allenai/go-swaggerui"
	"github.com/labstack/echo/v4"
)

// UIMiddleware creates a middleware for UI-related handlers that passes context
func UIMiddleware(_ context.Context) echo.MiddlewareFunc {
	uiHandler := http.StripPrefix("/oc3/api/docs", swaggerui.Handler("/oc3/api/docs/openapi"))
	echoUI := echo.WrapHandler(uiHandler)

	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			return echoUI(c)
		}
	}
}
