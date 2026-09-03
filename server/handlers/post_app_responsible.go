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
	"github.com/opensvc/oc3/xauth"
)

// PostAppResponsible handles POST /apps/{app_id}/responsibles/{group_id}
func (a *Api) PostAppResponsible(c echo.Context, appId string, groupId string) error {
	return a.postAppResponsible(c, "PostAppResponsible", appId, groupId)
}

func (a *Api) postAppResponsible(c echo.Context, handlerName, appId, groupId string) error {
	log := echolog.GetLogHandler(c, handlerName)
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

	user := UserInfoFromContext(c)
	if user == nil {
		return JSONProblemf(c, http.StatusUnauthorized, "missing user context")
	}
	userID, err := strconv.ParseInt(user.GetExtensions().Get(xauth.XUserID), 10, 64)
	if err != nil {
		return JSONProblemf(c, http.StatusBadRequest, "invalid user id")
	}
	userGroupIDs, err := odb.UserGroupIDs(ctx, userID)
	if err != nil {
		log.Error("cannot list user groups", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot list user groups")
	}

	group, status, err := odb.OrgGroup(ctx, groupId, userGroupIDs, isManager)
	if err != nil {
		log.Error("cannot resolve group", "group_id", groupId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve group %s", groupId)
	}
	switch status {
	case cdb.OrgGroupNotFound:
		return JSONProblemf(c, http.StatusNotFound, "group %s not found", groupId)
	case cdb.OrgGroupAmbiguous:
		return JSONProblemf(c, http.StatusBadRequest, "ambiguous group id: %s", groupId)
	case cdb.OrgGroupPrivileged:
		return JSONProblemf(c, http.StatusForbidden, "operation not allowed on privileged group: %s", group.Role)
	}

	exists, err := odb.AppResponsibleExists(ctx, app.ID, group.ID)
	if err != nil {
		log.Error("cannot check responsible", "app_id", appId, "group_id", groupId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check responsible")
	}
	if exists {
		return JSONProblemf(c, http.StatusConflict, "group %s is already responsible for app %s", group.Role, app.App)
	}

	if err := odb.InsertAppResponsible(ctx, app.ID, group.ID); err != nil {
		log.Error("cannot insert responsible", "app_id", appId, "group_id", groupId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot give responsibility for app %s to group %s", app.App, group.Role)
	}

	if err := odb.DeleteDashboardAppWithoutResponsible(ctx, app.App); err != nil {
		log.Error("cannot cleanup dashboard alerts", "app", app.App, logkey.Error, err)
	}

	msg := fmt.Sprintf("app %s responsibility given to group %s", app.App, group.Role)
	userEmail, _ := c.Get(XUserEmail).(string)
	if logErr := odb.Log(ctx, cdb.LogEntry{
		Action: "apps.responsible.add",
		User:   userEmail,
		Fmt:    "app %(u)s responsibility given to group %(g)s",
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
