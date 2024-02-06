package handlers

import (
	"context"
	"net/http"

	"github.com/allenai/go-swaggerui"
	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/api"
)

type (
	Api struct {
	}
)

func New() *Api {
	return &Api{}
}

func UIMiddleware(ctx context.Context) echo.MiddlewareFunc {
	uiHandler := http.StripPrefix("/public/ui", swaggerui.Handler("/public/openapi"))
	echoUI := echo.WrapHandler(uiHandler)

	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			return echoUI(c)
		}
	}
}

func (a *Api) GetSwagger(ctx echo.Context) error {
	swagger, err := api.GetSwagger()
	if err != nil {
		return nil
	}
	return ctx.JSON(http.StatusOK, swagger)
}
