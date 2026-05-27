package serverhandlers

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// DeleteService handles DELETE /services/{svc_id}
func (a *Api) DeleteService(c echo.Context, svcId string) error {
	log := echolog.GetLogHandler(c, "DeleteService")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}

	log.Info("called", "svc_id", svcId)

	svc, err := odb.ServiceBySvcIDOrName(ctx, svcId)
	if err != nil {
		log.Error("cannot lookup service", "svc_id", svcId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot lookup service %s", svcId)
	}
	if svc == nil {
		return JSONProblemf(c, http.StatusNotFound, "service %s not found", svcId)
	}

	responsible, err := odb.ServiceResponsible(ctx, svc.SvcID, UserGroupsFromContext(c), IsManager(c))
	if err != nil {
		log.Error("cannot check service responsibility", "svc_id", svc.SvcID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check service responsibility")
	}
	if !responsible {
		return JSONProblemf(c, http.StatusForbidden, "you are not responsible for service %s", svc.Svcname)
	}

	svcLabel := svc.Svcname
	if svc.SvcApp != "" {
		svcLabel = svc.Svcname + " in app " + svc.SvcApp
	} else {
		svcLabel = svc.Svcname + " in no app"
	}

	markSuccess, endTx, err := odb.BeginTxWithControl(ctx, log, &sql.TxOptions{})
	if err != nil {
		log.Error("cannot start transaction", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot delete service")
	}
	defer endTx()

	userEmail, _ := c.Get(XUserEmail).(string)
	if logErr := odb.Log(ctx, cdb.LogEntry{
		Action: "service.delete",
		User:   userEmail,
		Fmt:    "delete service %(data)s",
		Dict:   map[string]any{"data": svcLabel},
		Level:  "info",
	}); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot write audit log")
	}

	if err := odb.DeleteServiceCascade(ctx, svc.SvcID); err != nil {
		log.Error("cannot delete service", "svc_id", svc.SvcID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot delete service %s", svc.Svcname)
	}

	markSuccess()

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	return c.JSON(http.StatusOK, map[string]string{
		"info": fmt.Sprintf("service %s deleted", svcLabel),
	})
}
