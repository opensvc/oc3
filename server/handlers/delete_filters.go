package serverhandlers

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// DeleteFilters handles DELETE /filters
func (a *Api) DeleteFilters(c echo.Context) error {
	log := echolog.GetLogHandler(c, "DeleteFilters")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if err := requireCompManager(c); err != nil {
		return err
	}

	var body server.DeleteFiltersJSONRequestBody
	if err := c.Bind(&body); err != nil {
		log.Error("invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	if body.Id == "" {
		return JSONProblemf(c, http.StatusBadRequest, "The 'id' key is mandatory")
	}

	log.Info("called", "filter_id", body.Id)

	return a.deleteFilterByKey(c, log, ctx, body.Id)
}
