package serverhandlers

import (
	"context"
	"database/sql"
	"log/slog"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// DeleteFilter handles DELETE /filters/{filter_id}: delete a filter and its attachments to filtersets.
func (a *Api) DeleteFilter(c echo.Context, filterId string) error {
	log := echolog.GetLogHandler(c, "DeleteFilter")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if err := requireCompManager(c); err != nil {
		return err
	}

	log.Info("called", "filter_id", filterId)

	return a.deleteFilterByKey(c, log, ctx, filterId)
}

func (a *Api) deleteFilterByKey(c echo.Context, log *slog.Logger, ctx context.Context, filterId string) error {
	odb := a.ODB

	id, found, err := odb.FilterID(ctx, filterId)
	if err != nil {
		log.Error("cannot resolve filter", "filter_id", filterId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve filter")
	}
	if !found {
		return JSONProblemf(c, http.StatusNotFound, "filter %s not found", filterId)
	}

	row, err := odb.GetFilterRow(ctx, id)
	if err != nil {
		log.Error("cannot get filter", "filter_id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get filter")
	}
	if row == nil {
		return JSONProblemf(c, http.StatusNotFound, "filter %d not found", id)
	}

	markSuccess, endTx, err := odb.BeginTxWithControl(ctx, log, &sql.TxOptions{})
	if err != nil {
		log.Error("cannot start transaction", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot delete filter")
	}
	defer endTx()

	if err := odb.DeleteFilterCascade(ctx, id); err != nil {
		log.Error("cannot delete filter", "filter_id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot delete filter")
	}

	userEmail, _ := c.Get(XUserEmail).(string)
	if logErr := odb.Log(ctx, cdb.LogEntry{
		Action: "filter.delete",
		User:   userEmail,
		Fmt:    "deleted filter %(t)s.%(f)s %(o)s %(val)s",
		Dict: map[string]any{
			"t":   row.FTable,
			"f":   row.FField,
			"o":   row.FOp,
			"val": row.FValue,
		},
		Level: "info",
	}); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
	}

	markSuccess()

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	return c.JSON(http.StatusOK, map[string]string{"info": "filter deleted"})
}
