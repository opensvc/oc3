package serverhandlers

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

func (a *Api) GetVersion(c echo.Context) error {
	if SCHEMA.Info == nil {
		return JSONProblem(c, http.StatusInternalServerError, "invalid api schema", "missing schema info")
	}
	return c.JSON(http.StatusOK, map[string]string{
		"version": SCHEMA.Info.Version,
		"service": "server",
	})
}
