package serverhandlers

import (
	"context"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// GetUserHiddenMenuEntries handles GET /users/{user_id}/hidden_menu_entries:
// the menu entries hidden to the user, resolved as cdb.HiddenMenuEntriesOf
// does. For "self", the groups of the request, which follow an impersonation.
func (a *Api) GetUserHiddenMenuEntries(c echo.Context, userId string) error {
	log := echolog.GetLogHandler(c, "GetUserHiddenMenuEntries")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}
	roles := UserGroupsFromContext(c)
	if userId != "self" {
		id, err := strconv.ParseInt(userId, 10, 64)
		if err != nil {
			return JSONProblemf(c, http.StatusBadRequest, "user_id must be an auth_user.id or self")
		}
		if self := authUserID(c); self == nil || *self != id {
			if !IsManager(c) && !HasGroup(c, "UserManager") {
				return JSONProblemf(c, http.StatusForbidden, "UserManager privilege required")
			}
			if roles, err = a.ODB.UserRoles(ctx, id); err != nil {
				log.Error("cannot read the groups of the user", "user_id", id, logkey.Error, err)
				return JSONProblemf(c, http.StatusInternalServerError, "cannot read the groups of the user")
			}
		}
	}
	entries, err := a.ODB.HiddenMenuEntriesOf(ctx, roles)
	if err != nil {
		log.Error("cannot read the hidden menu entries", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read the hidden menu entries")
	}
	return c.JSON(http.StatusOK, server.HiddenMenuEntriesResponse{Data: entries})
}
