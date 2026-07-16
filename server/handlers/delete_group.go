package serverhandlers

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
	"github.com/opensvc/oc3/xauth"
)

// DeleteGroup handles DELETE /groups/{group_id}
func (a *Api) DeleteGroup(c echo.Context, groupId string) error {
	log := echolog.GetLogHandler(c, "DeleteGroup")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}
	if !IsGroupManager(c) {
		return JSONProblemf(c, http.StatusForbidden, "GroupManager privilege required")
	}

	log.Info("called", "group_id", groupId)

	isManager := IsManager(c)

	var userGroupIDs []int64
	if !isManager {
		user := UserInfoFromContext(c)
		if user == nil {
			return JSONProblemf(c, http.StatusUnauthorized, "missing user context")
		}
		userID, err := strconv.ParseInt(user.GetExtensions().Get(xauth.XUserID), 10, 64)
		if err != nil {
			return JSONProblemf(c, http.StatusBadRequest, "invalid user id")
		}
		userGroupIDs, err = odb.UserGroupIDs(ctx, userID)
		if err != nil {
			log.Error("cannot list user groups", logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot list user groups")
		}
	}

	group, err := odb.GroupForDelete(ctx, groupId, userGroupIDs, isManager)
	if err != nil {
		log.Error("cannot resolve group", "group_id", groupId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve group %s", groupId)
	}
	if group == nil {
		return c.JSON(http.StatusOK, map[string]string{
			"info": fmt.Sprintf("Group %s does not exists", groupId),
		})
	}
	if group.Role == "Everybody" {
		return JSONProblemf(c, http.StatusBadRequest, "the 'Everybody' group is immutable")
	}

	markSuccess, endTx, err := odb.BeginTxWithControl(ctx, log, &sql.TxOptions{})
	if err != nil {
		log.Error("cannot start transaction", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot delete group")
	}
	defer endTx()

	if err := odb.DeleteGroupCascade(ctx, group.ID); err != nil {
		log.Error("cannot delete group", "group_id", group.ID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot delete group %s", group.Role)
	}

	userEmail, _ := c.Get(XUserEmail).(string)
	if logErr := odb.Log(ctx, cdb.LogEntry{
		Action: "groups.delete",
		User:   userEmail,
		Fmt:    "deleted group %(g)s",
		Dict:   map[string]any{"g": group.Role},
		Level:  "info",
	}); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot write audit log")
	}

	markSuccess()

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	return c.JSON(http.StatusOK, map[string]string{
		"info": fmt.Sprintf("Group %s deleted", group.Role),
	})
}
