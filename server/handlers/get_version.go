package serverhandlers

import (
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/server"
)

func (a *Api) GetVersion(c echo.Context) error {
	if SCHEMA.Info == nil {
		return JSONProblem(c, http.StatusInternalServerError, "invalid api schema")
	}
	// api_docs: whether the Swagger UI is served (server.ui.enable), for the
	// interface to link it only then.
	service := "server"
	return c.JSON(http.StatusOK, server.Version{
		Version: SCHEMA.Info.Version,
		Service: &service,
		ApiDocs: &a.UI,
	})
}
