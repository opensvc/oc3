package serverhandlers

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// GetUserGroups handles GET /users/{user_id}/groups: the groups of a user the
// caller can see.
func (a *Api) GetUserGroups(c echo.Context, userId string, params server.GetUserGroupsParams) error {
	return a.handleList(c, "GetUserGroups", "auth_group", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
		filter: params.Filter, withUserID: true,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetUserGroups(ctx, userId, p)
	})
}

// PostUserGroup handles POST /users/{user_id}/groups/{group_id}.
func (a *Api) PostUserGroup(c echo.Context, userId string, groupId string) error {
	return a.changeUserGroup(c, "PostUserGroup", userId, groupId, true)
}

// DeleteUserGroup handles DELETE /users/{user_id}/groups/{group_id}.
func (a *Api) DeleteUserGroup(c echo.Context, userId string, groupId string) error {
	return a.changeUserGroup(c, "DeleteUserGroup", userId, groupId, false)
}

// changeUserGroup attaches a user to a group, or detaches them, with the rules
// of rest_post_user_group and rest_delete_user_group
// (init/models/rest/api_users.py:567): the GroupManager privilege, and for a
// caller who is not a Manager, a group they are member of.
func (a *Api) changeUserGroup(c echo.Context, name, userId, groupId string, attach bool) error {
	log := echolog.GetLogHandler(c, name)
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}
	if !IsGroupManager(c) {
		return JSONProblemf(c, http.StatusForbidden, "changing the groups of a user requires the GroupManager privilege")
	}
	user, err := odb.MembershipUserByIdent(ctx, userId)
	if err != nil {
		log.Error("cannot resolve user", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve user %s", userId)
	}
	if user == nil {
		return JSONProblemf(c, http.StatusNotFound, "user %s does not exist", userId)
	}
	group, err := odb.MembershipGroupByIdent(ctx, groupId, UserGroupsFromContext(c), IsManager(c))
	if err != nil {
		log.Error("cannot resolve group", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve group %s", groupId)
	}
	if group == nil {
		if IsManager(c) {
			return JSONProblemf(c, http.StatusNotFound, "group %s does not exist", groupId)
		}
		return JSONProblemf(c, http.StatusNotFound, "group %s does not exist or you are not member of it", groupId)
	}
	exists, err := odb.MembershipExists(ctx, user.ID, group.ID)
	if err != nil {
		log.Error("cannot check membership", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check the membership")
	}

	var action, format, info string
	switch {
	case attach && exists:
		return c.JSON(http.StatusOK, map[string]string{"info": "user " + user.Email + " is already member of group " + group.Role})
	case !attach && !exists:
		return c.JSON(http.StatusOK, map[string]string{"info": "user " + user.Email + " is already detached from group " + group.Role})
	case attach:
		if err := odb.InsertGroupMembership(ctx, group.ID, user.ID); err != nil {
			log.Error("cannot attach", logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot attach the user to the group")
		}
		action, format = "user.group.attach", "user %(u)s attached to group %(g)s"
		info = "user " + user.Email + " attached to group " + group.Role
	default:
		if err := odb.DeleteGroupMembership(ctx, user.ID, group.ID); err != nil {
			log.Error("cannot detach", logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot detach the user from the group")
		}
		action, format = "user.group.detach", "user %(u)s detached from group %(g)s"
		info = "user " + user.Email + " detached from group " + group.Role
	}

	userEmail, _ := c.Get(XUserEmail).(string)
	if logErr := odb.Log(ctx, cdb.LogEntry{
		Action: action,
		User:   userEmail,
		Fmt:    format,
		Dict:   map[string]any{"u": user.Email, "g": group.Role},
		Level:  "info",
	}); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
	}
	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}
	return c.JSON(http.StatusOK, map[string]string{"info": info})
}
