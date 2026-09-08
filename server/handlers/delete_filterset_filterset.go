package serverhandlers

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// DeleteFiltersetFilterset handles DELETE /filtersets/{filterset_id}/filtersets/{child_id}:
// detach the encapsulated child filterset from the parent filterset.
func (a *Api) DeleteFiltersetFilterset(c echo.Context, filtersetId string, childId string) error {
	log := echolog.GetLogHandler(c, "DeleteFiltersetFilterset")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if err := requireCompManager(c); err != nil {
		return err
	}

	log.Info("called", "filterset_id", filtersetId, "child_id", childId)

	return a.deleteFiltersetFilterset(c, log, ctx, filtersetId, childId)
}

func (a *Api) deleteFiltersetFilterset(c echo.Context, log *slog.Logger, ctx context.Context, filtersetId, childId string) error {
	odb := a.ODB

	parentID, found, err := odb.FiltersetID(ctx, filtersetId)
	if err != nil {
		log.Error("cannot resolve parent filterset", "filterset_id", filtersetId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve filterset")
	}
	if !found {
		return JSONProblemf(c, http.StatusNotFound, "parent filterset %s not found", filtersetId)
	}

	childID, found, err := odb.FiltersetID(ctx, childId)
	if err != nil {
		log.Error("cannot resolve child filterset", "child_id", childId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve filterset")
	}
	if !found {
		return JSONProblemf(c, http.StatusNotFound, "child filterset %s not found", childId)
	}

	parent, err := odb.GetFiltersetRow(ctx, parentID)
	if err != nil {
		log.Error("cannot get parent filterset", "filterset_id", parentID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get filterset")
	}
	if parent == nil {
		return JSONProblemf(c, http.StatusNotFound, "parent filterset %d not found", parentID)
	}

	child, err := odb.GetFiltersetRow(ctx, childID)
	if err != nil {
		log.Error("cannot get child filterset", "child_id", childID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get filterset")
	}
	if child == nil {
		return JSONProblemf(c, http.StatusNotFound, "child filterset %d not found", childID)
	}

	n, err := odb.DetachFiltersetFromFilterset(ctx, parentID, childID)
	if err != nil {
		log.Error("cannot detach filterset", "filterset_id", parentID, "child_id", childID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot detach filterset")
	}
	if n == 0 {
		return c.JSON(http.StatusOK, map[string]string{"info": "filterset already detached"})
	}

	userEmail, _ := c.Get(XUserEmail).(string)
	if logErr := odb.Log(ctx, cdb.LogEntry{
		Action: "filterset.detach",
		User:   userEmail,
		Fmt:    "detach filterset %(fset_name)s from filterset %(parent_fset_name)s",
		Dict:   map[string]any{"fset_name": child.FsetName, "parent_fset_name": parent.FsetName},
		Level:  "info",
	}); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
	}

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	return c.JSON(http.StatusOK, map[string]string{"info": "filterset detached"})
}
