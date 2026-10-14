package serverhandlers

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"

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
			return denyRequest(c, http.StatusForbidden, "Manager privilege required")
		}
		return nil
	}
	if !IsGroupManager(c) {
		return denyRequest(c, http.StatusForbidden, "GroupManager privilege required")
	}
	return nil
}

// groupRoleMaxLen is the size of the auth_group.role column.
const groupRoleMaxLen = 255

// PostGroup handles POST /groups/{group_id}: it renames a group, changes its
// description or makes it a privilege group or an organizational one. The name
// is trimmed, and refused when empty, too long or carried by another group. A
// privilege group, and the privilege flag, are changed by a Manager only.
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
		if isManager {
			return JSONProblemf(c, http.StatusNotFound, "group %s does not exist", groupId)
		}
		return JSONProblemf(c, http.StatusNotFound, "group %s does not exist or you are not member of it", groupId)
	}
	if group.Role == "Everybody" {
		return JSONProblemf(c, http.StatusBadRequest, "the 'Everybody' group is immutable")
	}
	// A privilege group grants its privilege by its name: renaming it, or making
	// it an organizational group, is for a Manager, as making a group a privilege
	// one is.
	if group.Privilege && !isManager {
		return denyRequest(c, http.StatusForbidden, "changing a privilege group requires the Manager privilege")
	}
	if privilege != nil && *privilege != "T" && *privilege != "F" {
		return JSONProblemf(c, http.StatusBadRequest, "privilege must be T or F")
	}
	if role != nil {
		name := strings.TrimSpace(*role)
		switch {
		case name == "":
			return JSONProblemf(c, http.StatusBadRequest, "the group name may not be empty")
		case utf8.RuneCountInString(name) > groupRoleMaxLen:
			return JSONProblemf(c, http.StatusBadRequest, "the group name may not exceed %d characters", groupRoleMaxLen)
		}
		// role is unique: refused here with its reason rather than by the database.
		otherID, taken, err := odb.GroupIDByRole(ctx, name)
		if err != nil {
			log.Error("cannot check group existence", "role", name, logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot check group existence")
		}
		if taken && otherID != group.ID {
			return JSONProblemf(c, http.StatusConflict, "group '%s' already exists", name)
		}
		role = &name
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
			// Read whatever the caller's groups: a non-Manager sees their groups by
			// the names of their authentication, which still hold the former name
			// of a group they just renamed. They may change it, so may read it.
			p.IsManager = true
			return odb.GetGroup(ctx, strconv.FormatInt(group.ID, 10), p)
		})
}
