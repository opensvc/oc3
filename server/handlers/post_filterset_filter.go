package serverhandlers

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// PostFiltersetFilter handles POST /filtersets/{filterset_id}/filters/{f_id}: attach a filter to a filterset.
func (a *Api) PostFiltersetFilter(c echo.Context, filtersetId string, fId string) error {
	log := echolog.GetLogHandler(c, "PostFiltersetFilter")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if err := requireCompManager(c); err != nil {
		return err
	}

	var body server.PostFiltersetFilterJSONRequestBody
	if err := c.Bind(&body); err != nil {
		log.Error("invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	log.Info("called", "filterset_id", filtersetId, "f_id", fId)

	return a.postFiltersetFilter(c, log, ctx, filtersetId, fId, body.FLogOp, body.FOrder)
}

func (a *Api) postFiltersetFilter(c echo.Context, log *slog.Logger, ctx context.Context, filtersetId, fId string, fLogOp *string, fOrder *int) error {
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

	current, err := odb.GetFiltersetFilterAttachment(ctx, fsetID, id)
	if err != nil {
		log.Error("cannot get filter attachment", "filterset_id", fsetID, "f_id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get filter attachment")
	}

	if current != nil &&
		(fOrder == nil || current.FOrder == *fOrder) &&
		(fLogOp == nil || current.FLogOp == *fLogOp) {
		return c.JSON(http.StatusOK, map[string]string{
			"info": fmt.Sprintf("filter %d already attached to filterset %d", id, fsetID),
		})
	}

	if current != nil {
		if err := odb.UpdateFiltersetFilter(ctx, fsetID, id, fOrder, fLogOp); err != nil {
			log.Error("cannot update filter attachment", "filterset_id", fsetID, "f_id", id, logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot update filter attachment")
		}
	} else {
		order := 0
		if fOrder != nil {
			order = *fOrder
		}
		logOp := "AND"
		if fLogOp != nil {
			logOp = *fLogOp
		}
		if err := odb.InsertFiltersetFilter(ctx, fsetID, id, order, logOp); err != nil {
			log.Error("cannot attach filter to filterset", "filterset_id", fsetID, "f_id", id, logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot attach filter to filterset")
		}
	}

	fName := fmt.Sprintf("%s.%s %s %s", filter.FTable, filter.FField, filter.FOp, filter.FValue)
	userEmail, _ := c.Get(XUserEmail).(string)
	if logErr := odb.Log(ctx, cdb.LogEntry{
		Action: "filter.attach",
		User:   userEmail,
		Fmt:    "attach filter %(f_name)s to filterset %(fset_name)s",
		Dict:   map[string]any{"f_name": fName, "fset_name": fset.FsetName},
		Level:  "info",
	}); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
	}

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	return c.JSON(http.StatusOK, map[string]string{
		"info": fmt.Sprintf("filter %d attached to filterset %d", id, fsetID),
	})
}
