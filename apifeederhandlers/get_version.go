package apifeederhandlers

import (
	"net/http"

	"github.com/labstack/echo/v4"

	api "github.com/opensvc/oc3/apifeeder"
)

func (a *Api) GetVersion(c echo.Context) error {
	if SCHEMA.Info == nil {
		return JSONProblem(c, http.StatusInternalServerError, "invalid api schema", "missing schema info")
	}
	return c.JSON(http.StatusOK, api.Version{Version: SCHEMA.Info.Version})
}
