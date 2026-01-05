package apihandlers

import (
	"context"
	"net/http"

	"github.com/allenai/go-swaggerui"
	"github.com/labstack/echo/v4"
)

func UIMiddleware(_ context.Context) echo.MiddlewareFunc {
	uiHandler := http.StripPrefix("/oc3/docs", swaggerui.Handler("/oc3/docs/openapi"))
	echoUI := echo.WrapHandler(uiHandler)

	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			return echoUI(c)
		}
	}
}
