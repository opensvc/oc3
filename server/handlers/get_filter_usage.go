package serverhandlers

import (
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// GetFilterUsage handles GET /filters/{filter_id}/usage: the filtersets holding a
// filter, which lose it when the filter is deleted.
func (a *Api) GetFilterUsage(c echo.Context, filterId string) error {
	log := echolog.GetLogHandler(c, "GetFilterUsage")
	odb := a.ODB
	ctx := c.Request().Context()

	id, found, err := odb.FilterID(ctx, filterId)
	if err != nil {
		log.Error("cannot resolve filter", "filter_id", filterId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve filter")
	}
	if !found {
		return JSONProblemf(c, http.StatusNotFound, "filter %s not found", filterId)
	}
	// FilterID takes a number for an id without looking it up.
	row, err := odb.GetFilterRow(ctx, id)
	if err != nil {
		log.Error("cannot get filter", "filter_id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get filter")
	}
	if row == nil {
		return JSONProblemf(c, http.StatusNotFound, "filter %s not found", filterId)
	}

	fsets, err := odb.FilterUsageFiltersets(ctx, id)
	if err != nil {
		log.Error("cannot fetch filtersets", "filter_id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot fetch filter usage")
	}
	return c.JSON(http.StatusOK, map[string]any{
		"data": map[string]any{"filtersets": fsets},
	})
}
