package serverhandlers

import (
	"context"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// DeleteAppResponsible handles DELETE /apps/{app_id}/responsibles/{group_id}
func (a *Api) DeleteAppResponsible(c echo.Context, appId string, groupId string) error {
	log := echolog.GetLogHandler(c, "DeleteAppResponsible")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}

	isManager := IsManager(c)
	if !isManager {
		return JSONProblemf(c, http.StatusForbidden, "AppManager privilege required")
	}

	log.Info("called", "app_id", appId, "group_id", groupId)

	app, err := odb.GetApp(ctx, appId, nil, true)
	if err != nil {
		log.Error("cannot resolve app", "app_id", appId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve app %s", appId)
	}
	if app == nil {
		return JSONProblemf(c, http.StatusNotFound, "app %s not found", appId)
	}

	responsible, err := odb.AppResponsible(ctx, appId, UserGroupsFromContext(c), isManager, "")
	if err != nil {
		log.Error("cannot check app responsibility", "app_id", appId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check app responsibility")
	}
	if !responsible {
		return JSONProblemf(c, http.StatusForbidden, "you are not responsible for app %s", app.App)
	}

	group, found, err := odb.AuthGroupByIDOrRole(ctx, groupId)
	if err != nil {
		log.Error("cannot resolve group", "group_id", groupId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve group %s", groupId)
	}
	if !found {
		return JSONProblemf(c, http.StatusNotFound, "group %s not found", groupId)
	}

	n, err := odb.DeleteAppResponsible(ctx, app.ID, group.ID)
	if err != nil {
		log.Error("cannot delete responsible", "app_id", appId, "group_id", groupId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot revoke responsibility for app %s from group %s", app.App, group.Role)
	}
	if n == 0 {
		return JSONProblemf(c, http.StatusNotFound, "group %s is not responsible for app %s", group.Role, app.App)
	}

	msg := fmt.Sprintf("app %s responsibility revoked from group %s", app.App, group.Role)
	userEmail, _ := c.Get(XUserEmail).(string)
	if logErr := odb.Log(ctx, cdb.LogEntry{
		Action: "apps.responsible.delete",
		User:   userEmail,
		Fmt:    "app %(u)s responsibility revoked from group %(g)s",
		Dict: map[string]any{
			"u": app.App,
			"g": group.Role,
		},
		Level: "info",
	}); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
	}

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	return c.JSON(http.StatusOK, map[string]string{"info": msg})
}
