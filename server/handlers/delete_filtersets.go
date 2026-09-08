package serverhandlers

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// DeleteFiltersets handles DELETE /filtersets
func (a *Api) DeleteFiltersets(c echo.Context) error {
	log := echolog.GetLogHandler(c, "DeleteFiltersets")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if err := requireCompManager(c); err != nil {
		return err
	}

	var body server.DeleteFiltersetsJSONRequestBody
	if err := c.Bind(&body); err != nil {
		log.Error("invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	if body.Id == "" {
		return JSONProblemf(c, http.StatusBadRequest, "The 'id' key is mandatory")
	}

	log.Info("called", "filterset_id", body.Id)

	return a.deleteFiltersetByKey(c, log, ctx, body.Id)
}
