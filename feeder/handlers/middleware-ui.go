package feederhandlers

import (
	"context"
	"net/http"

	"github.com/allenai/go-swaggerui"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

func UIMiddleware(_ context.Context, prefix, specUrl string) echo.MiddlewareFunc {
	uiHandler := http.StripPrefix(prefix, swaggerui.Handler(specUrl))
	echoUI := echo.WrapHandler(uiHandler)

	middleware.WWWRedirect()

	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			return echoUI(c)
		}
	}
}
