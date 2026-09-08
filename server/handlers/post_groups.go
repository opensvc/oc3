package serverhandlers

import (
	"context"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
	"github.com/opensvc/oc3/xauth"
)

// PostGroups handles POST /groups
func (a *Api) PostGroups(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PostGroups")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}

	var body server.PostGroupsJSONRequestBody
	if err := c.Bind(&body); err != nil {
		log.Error("invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	// If 'id' or 'role' matches an existing group, this is an update.
	var lookupKey string
	if body.Id != nil && *body.Id != "" {
		lookupKey = *body.Id
	} else if body.Role != nil && *body.Role != "" {
		lookupKey = *body.Role
	}
	if lookupKey != "" {
		existing, ok, err := odb.AuthGroupByIDOrRole(ctx, lookupKey)
		if err != nil {
			log.Error("cannot resolve group", logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve group")
		}
		if ok {
			return a.postGroupUpdate(c, ctx, log, strconv.FormatInt(existing.ID, 10), body.Role, body.Description, body.Privilege)
		}
	}

	if err := requireGroupPrivilege(c, body.Privilege); err != nil {
		return err
	}

	if body.Role == nil || *body.Role == "" {
		return JSONProblemf(c, http.StatusBadRequest, "role is mandatory")
	}

	log.Info("called", "role", *body.Role)

	user := UserInfoFromContext(c)
	if user == nil {
		return JSONProblemf(c, http.StatusUnauthorized, "missing user context")
	}
	userID, err := strconv.ParseInt(user.GetExtensions().Get(xauth.XUserID), 10, 64)
	if err != nil {
		return JSONProblemf(c, http.StatusBadRequest, "invalid user id")
	}

	exceeded, err := odb.GroupQuotaExceeded(ctx, userID)
	if err != nil {
		log.Error("cannot check group quota", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check group quota")
	}
	if exceeded {
		return JSONProblemf(c, http.StatusForbidden, "org group quota exceeded")
	}

	var description, privilege string
	if body.Description != nil {
		description = *body.Description
	}
	if body.Privilege != nil {
		privilege = *body.Privilege
	}

	group, err := odb.InsertGroup(ctx, *body.Role, description, privilege)
	if err != nil {
		log.Error("cannot insert group", "role", *body.Role, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot create group")
	}

	if err := odb.InsertGroupMembership(ctx, group.ID, userID); err != nil {
		log.Error("cannot insert group membership", "role", *body.Role, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot set group membership")
	}

	userEmail, _ := c.Get(XUserEmail).(string)
	logErr := odb.Log(ctx, cdb.LogEntry{
		Action: "groups.add",
		User:   userEmail,
		Fmt:    "add group %(role)s",
		Dict:   map[string]any{"role": group.Role},
		Level:  "info",
	})
	if logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
	}

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	return a.handleItem(c, "PostGroups", "auth_group", "id", strconv.FormatInt(group.ID, 10), listEndpointParams{},
		func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
			return odb.GetGroup(ctx, strconv.FormatInt(group.ID, 10), p)
		})
}
