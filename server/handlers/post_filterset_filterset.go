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

// PostFiltersetFilterset handles POST /filtersets/{filterset_id}/filtersets/{child_id}:
// encapsulate the child filterset into the parent filterset.
func (a *Api) PostFiltersetFilterset(c echo.Context, filtersetId string, childId string) error {
	log := echolog.GetLogHandler(c, "PostFiltersetFilterset")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if err := requireCompManager(c); err != nil {
		return err
	}

	var body server.PostFiltersetFiltersetJSONRequestBody
	if err := c.Bind(&body); err != nil {
		log.Error("invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	log.Info("called", "filterset_id", filtersetId, "child_id", childId)

	return a.postFiltersetFilterset(c, log, ctx, filtersetId, childId, body.FLogOp, body.FOrder)
}

func (a *Api) postFiltersetFilterset(c echo.Context, log *slog.Logger, ctx context.Context, filtersetId, childId string, fLogOp *string, fOrder *int) error {
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

	current, err := odb.GetFiltersetEncapAttachment(ctx, parentID, childID)
	if err != nil {
		log.Error("cannot get encapsulation", "filterset_id", parentID, "child_id", childID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get encapsulation")
	}

	if current != nil &&
		(fOrder == nil || current.FOrder == *fOrder) &&
		(fLogOp == nil || current.FLogOp == *fLogOp) {
		return c.JSON(http.StatusOK, map[string]string{
			"info": fmt.Sprintf("filterset %d already attached to filterset %d", childID, parentID),
		})
	}

	if current != nil {
		if err := odb.UpdateFiltersetEncap(ctx, parentID, childID, fOrder, fLogOp); err != nil {
			log.Error("cannot update encapsulation", "filterset_id", parentID, "child_id", childID, logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot update encapsulation")
		}
	} else {
		loop, err := odb.FiltersetEncapWouldLoop(ctx, childID, parentID)
		if err != nil {
			log.Error("cannot check encapsulation loop", "filterset_id", parentID, "child_id", childID, logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot check encapsulation loop")
		}
		if loop {
			return JSONProblemf(c, http.StatusConflict, "the parent filterset is already a child of the encapsulated filterset. abort encapsulation not to cause infinite recursion")
		}
		order := 0
		if fOrder != nil {
			order = *fOrder
		}
		logOp := "AND"
		if fLogOp != nil {
			logOp = *fLogOp
		}
		if err := odb.InsertFiltersetEncap(ctx, parentID, childID, order, logOp); err != nil {
			log.Error("cannot encapsulate filterset", "filterset_id", parentID, "child_id", childID, logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot encapsulate filterset")
		}
	}

	userEmail, _ := c.Get(XUserEmail).(string)
	if logErr := odb.Log(ctx, cdb.LogEntry{
		Action: "filterset.attach",
		User:   userEmail,
		Fmt:    "attach filterset %(fset_name)s to filterset %(dst_fset_name)s",
		Dict:   map[string]any{"fset_name": child.FsetName, "dst_fset_name": parent.FsetName},
		Level:  "info",
	}); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
	}

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	return c.JSON(http.StatusOK, map[string]string{"info": "filterset attached"})
}
