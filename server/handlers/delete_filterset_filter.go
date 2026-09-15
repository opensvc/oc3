package serverhandlers

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// DeleteFiltersetFilter handles DELETE /filtersets/{filterset_id}/filters/{f_id}: detach a filter from a filterset.
func (a *Api) DeleteFiltersetFilter(c echo.Context, filtersetId string, fId string) error {
	log := echolog.GetLogHandler(c, "DeleteFiltersetFilter")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if err := requireCompManager(c); err != nil {
		return err
	}

	log.Info("called", "filterset_id", filtersetId, "f_id", fId)

	return a.deleteFiltersetFilter(c, log, ctx, filtersetId, fId)
}

// deleteFiltersetFilter detaches the filter from the filterset.
func (a *Api) deleteFiltersetFilter(c echo.Context, log *slog.Logger, ctx context.Context, filtersetId, fId string) error {
	odb := a.ODB

	fsetID, found, err := odb.FiltersetID(ctx, filtersetId)
	if err != nil {
		log.Error("cannot resolve filterset", "filterset_id", filtersetId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve filterset")
	}
	if !found {
		return JSONProblemf(c, http.StatusNotFound, "filterset %s not found", filtersetId)
	}

	id, found, err := odb.FilterID(ctx, fId)
	if err != nil {
		log.Error("cannot resolve filter", "f_id", fId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve filter")
	}
	if !found {
		return JSONProblemf(c, http.StatusNotFound, "filter %s not found", fId)
	}

	filter, err := odb.GetFilterRow(ctx, id)
	if err != nil {
		log.Error("cannot get filter", "f_id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get filter")
	}
	if filter == nil {
		return JSONProblemf(c, http.StatusNotFound, "filter %d not found", id)
	}

	fset, err := odb.GetFiltersetRow(ctx, fsetID)
	if err != nil {
		log.Error("cannot get filterset", "filterset_id", fsetID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get filterset")
	}
	if fset == nil {
		return JSONProblemf(c, http.StatusNotFound, "filterset %d not found", fsetID)
	}

	n, err := odb.DetachFilterFromFilterset(ctx, fsetID, id)
	if err != nil {
		log.Error("cannot detach filter from filterset", "filterset_id", fsetID, "f_id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot detach filter from filterset")
	}
	if n == 0 {
		return c.JSON(http.StatusOK, map[string]string{"info": "filter already detached"})
	}

	fName := fmt.Sprintf("%s.%s %s %s", filter.FTable, filter.FField, filter.FOp, filter.FValue)
	userEmail, _ := c.Get(XUserEmail).(string)
	if logErr := odb.Log(ctx, cdb.LogEntry{
		Action: "filter.detach",
		User:   userEmail,
		Fmt:    "detach filter %(f_name)s from filterset %(fset_name)s",
		Dict:   map[string]any{"f_name": fName, "fset_name": fset.FsetName},
		Level:  "info",
	}); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
	}

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	return c.JSON(http.StatusOK, map[string]string{"info": "filter detached"})
}
