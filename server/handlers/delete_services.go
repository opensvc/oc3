package serverhandlers

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// DeleteServices handles DELETE /services
func (a *Api) DeleteServices(c echo.Context) error {
	log := echolog.GetLogHandler(c, "DeleteServices")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}

	var body server.DeleteServicesJSONRequestBody
	if err := c.Bind(&body); err != nil {
		log.Error("invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	if body.SvcId == "" {
		return JSONProblemf(c, http.StatusBadRequest, "The 'svc_id' key must be specified")
	}

	log.Info("called", "svc_id", body.SvcId)

	return a.deleteServiceByKey(c, log, ctx, body.SvcId)
}
