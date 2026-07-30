package serverhandlers

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// DeleteAlert handles DELETE /alerts/{id}
func (a *Api) DeleteAlert(c echo.Context, id string) error {
	return a.deleteAlertByID(c, "DeleteAlert", id)
}

func (a *Api) deleteAlertByID(c echo.Context, handlerName, id string) error {
	log := echolog.GetLogHandler(c, handlerName)
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if !IsAuthByUser(c) && !IsAuthByNode(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "authentication required")
	}

	alertID, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return JSONProblemf(c, http.StatusNotFound, "Alert %s not found", id)
	}

	_, nodeID, found, err := odb.GetAlertOwner(ctx, alertID)
	if err != nil {
		log.Error("cannot lookup alert", "alert_id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot lookup alert %s", id)
	}
	if !found {
		return JSONProblemf(c, http.StatusNotFound, "Alert %s not found", id)
	}

	if IsAuthByNode(c) {
		callerNodeID, _ := c.Get(XNodeID).(string)
		if nodeID != callerNodeID {
			return JSONProblemf(c, http.StatusForbidden, "the alert is not assigned to this node")
		}
	} else if !IsAlertsManager(c) {
		return JSONProblemf(c, http.StatusForbidden, "user has no AlertsManager privilege")
	}

	if _, err := odb.DeleteAlert(ctx, alertID); err != nil {
		log.Error("cannot delete alert", "alert_id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot delete alert %s", id)
	}

	userEmail, _ := c.Get(XUserEmail).(string)
	if logErr := odb.Log(ctx, cdb.LogEntry{
		Action: "alert.del",
		User:   userEmail,
		Fmt:    "Alert %(id)s deleted",
		Dict:   map[string]any{"id": id},
		Level:  "info",
	}); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
	}

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	return c.JSON(http.StatusOK, map[string]string{"info": fmt.Sprintf("Alert %s deleted", id)})
}
