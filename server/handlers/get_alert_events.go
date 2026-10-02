package serverhandlers

import (
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

const (
	alertEventsDefaultLimit = 1000
	alertEventsMaxLimit     = 10000
)

// GetAlertEvents handles GET /alerts/{id}/events: the past occurrences of an
// alert visible to the caller, for its timeline.
func (a *Api) GetAlertEvents(c echo.Context, id string, params server.GetAlertEventsParams) error {
	log := echolog.GetLogHandler(c, "GetAlertEvents")
	limit := alertEventsDefaultLimit
	if params.Limit != nil {
		if *params.Limit < 1 {
			return JSONProblemf(c, http.StatusBadRequest, "limit must be positive")
		}
		limit = min(*params.Limit, alertEventsMaxLimit)
	}
	events, found, truncated, err := a.ODB.GetAlertEventsOf(c.Request().Context(), id, UserGroupsFromContext(c), IsManager(c), limit)
	if err != nil {
		log.Error("cannot read the alert events", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read the alert events")
	}
	if !found {
		return JSONProblemf(c, http.StatusNotFound, "alert %s not found", id)
	}
	data := make([]server.AlertEvent, 0, len(events))
	for _, e := range events {
		data = append(data, server.AlertEvent{Id: int(e.ID), Begin: e.Begin, End: e.End})
	}
	resp := map[string]any{"data": data}
	if truncated {
		resp["truncated"] = true
	}
	return c.JSON(http.StatusOK, resp)
}
