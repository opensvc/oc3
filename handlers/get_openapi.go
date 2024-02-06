package handlers

import (
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/api"
)

func (a *Api) GetSwagger(ctx echo.Context) error {
	swagger, err := api.GetSwagger()
	if err != nil {
		return nil
	}
	return ctx.JSON(http.StatusOK, swagger)
}
