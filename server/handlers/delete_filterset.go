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

// DeleteFilterset handles DELETE /filtersets/{filterset_id}: delete a filterset and its attachments and references.
func (a *Api) DeleteFilterset(c echo.Context, filtersetId string) error {
	log := echolog.GetLogHandler(c, "DeleteFilterset")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if err := requireCompManager(c); err != nil {
		return err
	}

	log.Info("called", "filterset_id", filtersetId)

	return a.deleteFiltersetByKey(c, log, ctx, filtersetId)
}

func (a *Api) deleteFiltersetByKey(c echo.Context, log *slog.Logger, ctx context.Context, filtersetId string) error {
	odb := a.ODB

	id, found, err := odb.FiltersetID(ctx, filtersetId)
	if err != nil {
		log.Error("cannot resolve filterset", "filterset_id", filtersetId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve filterset")
	}
	if !found {
		return JSONProblemf(c, http.StatusNotFound, "filterset %s not found", filtersetId)
	}

	row, err := odb.GetFiltersetRow(ctx, id)
	if err != nil {
		log.Error("cannot get filterset", "filterset_id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get filterset")
	}
	if row == nil {
		return JSONProblemf(c, http.StatusNotFound, "filterset %d not found", id)
	}

	tx, markSuccess, endTx, err := odb.BeginTxWithControl(ctx, log, &sql.TxOptions{})
	if err != nil {
		log.Error("cannot start transaction", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot delete filterset")
	}
	defer endTx()

	if err := tx.DeleteFiltersetCascade(ctx, id); err != nil {
		log.Error("cannot delete filterset", "filterset_id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot delete filterset")
	}

	userEmail, _ := c.Get(XUserEmail).(string)
	if logErr := tx.Log(ctx, cdb.LogEntry{
		Action: "filterset.delete",
		User:   userEmail,
		Fmt:    "deleted filterset %(fset_name)s",
		Dict:   map[string]any{"fset_name": row.FsetName},
		Level:  "info",
	}); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
	}

	markSuccess()

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	return c.JSON(http.StatusOK, map[string]string{"info": "filterset deleted"})
}
