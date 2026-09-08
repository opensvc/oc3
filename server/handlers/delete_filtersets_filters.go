package serverhandlers

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// DeleteFiltersetsFilters handles DELETE /filtersets_filters
func (a *Api) DeleteFiltersetsFilters(c echo.Context) error {
	log := echolog.GetLogHandler(c, "DeleteFiltersetsFilters")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if err := requireCompManager(c); err != nil {
		return err
	}

	var body server.DeleteFiltersetsFiltersJSONRequestBody
	if err := c.Bind(&body); err != nil {
		log.Error("invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	if body.FsetId == "" {
		return JSONProblemf(c, http.StatusBadRequest, "The 'fset_id' key is mandatory")
	}
	if body.FId == "" {
		return JSONProblemf(c, http.StatusBadRequest, "The 'f_id' key is mandatory")
	}

	log.Info("called", "filterset_id", body.FsetId, "f_id", body.FId)

	return a.deleteFiltersetFilter(c, log, ctx, body.FsetId, body.FId)
}
