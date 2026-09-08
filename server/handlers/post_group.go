package serverhandlers

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
	"github.com/opensvc/oc3/xauth"
)

func requireGroupPrivilege(c echo.Context, privilege *string) error {
	if privilege != nil && (*privilege == "T" || *privilege == "true") {
		if !IsManager(c) {
			return JSONProblemf(c, http.StatusForbidden, "Manager privilege required")
		}
		return nil
	}
	if !IsGroupManager(c) {
		return JSONProblemf(c, http.StatusForbidden, "GroupManager privilege required")
	}
	return nil
}

// PostGroup handles POST /groups/{group_id}
func (a *Api) PostGroup(c echo.Context, groupId string) error {
	log := echolog.GetLogHandler(c, "PostGroup")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}

	var body server.PostGroupJSONRequestBody
	if err := c.Bind(&body); err != nil {
		log.Error("invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	return a.postGroupUpdate(c, ctx, log, groupId, body.Role, body.Description, body.Privilege)
}

func (a *Api) postGroupUpdate(c echo.Context, ctx context.Context, log *slog.Logger, groupId string, role, description, privilege *string) error {
	odb := a.ODB

	if err := requireGroupPrivilege(c, privilege); err != nil {
		return err
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

	group, err := odb.GroupForUpdate(ctx, groupId, userGroupIDs, isManager)
	if err != nil {
		log.Error("cannot resolve group", "group_id", groupId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve group %s", groupId)
	}
	if group == nil {
		return c.JSON(http.StatusOK, map[string]string{
			"error": fmt.Sprintf("Group %s does not exist", groupId),
		})
	}
	if group.Role == "Everybody" {
		return JSONProblemf(c, http.StatusBadRequest, "the 'Everybody' group is immutable")
	}

	fields := cdb.UpdateGroupFields{
		Role:        role,
		Description: description,
		Privilege:   privilege,
	}
	if err := odb.UpdateGroup(ctx, group.ID, fields); err != nil {
		log.Error("cannot update group", "group_id", group.ID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot update group")
	}

	userEmail, _ := c.Get(XUserEmail).(string)
	if err := odb.Log(ctx, cdb.LogEntry{
		Action: "groups.change",
		User:   userEmail,
		Fmt:    "change group %(role)s",
		Dict:   map[string]any{"role": group.Role},
		Level:  "info",
	}); err != nil {
		log.Error("cannot write audit log", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot write audit log")
	}

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	newGroupId := groupId
	if role != nil {
		newGroupId = *role
	}
	return a.handleItem(c, "PostGroup", "auth_group", "id", newGroupId, listEndpointParams{},
		func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
			return odb.GetGroup(ctx, strconv.FormatInt(group.ID, 10), p)
		})
}
