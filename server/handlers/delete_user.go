package serverhandlers

import (
	"context"
	"database/sql"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// DeleteUser handles DELETE /users/{user_id}.
func (a *Api) DeleteUser(c echo.Context, userId string) error {
	log := echolog.GetLogHandler(c, "DeleteUser")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()
	odb := a.ODB

	if err := requireUserManager(c); err != nil {
		return err
	}
	selfID := authUserID(c)
	if selfID == nil {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}
	if userId == "self" {
		return JSONProblemf(c, http.StatusConflict, "you cannot delete your own account")
	}
	targetID, found, err := odb.UserIDForPrefs(ctx, userId, cdb.ListParams{
		Groups: UserGroupsFromContext(c), IsManager: IsManager(c), UserID: selfID,
	})
	if err != nil {
		log.Error("cannot resolve user", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve the user")
	}
	if !found {
		return JSONProblemf(c, http.StatusNotFound, "user %s not found", userId)
	}
	if targetID == *selfID {
		return JSONProblemf(c, http.StatusConflict, "you cannot delete your own account")
	}
	current, found, err := odb.GetUserIdentity(ctx, targetID)
	if err != nil {
		log.Error("cannot read user", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read the user")
	}
	if !found {
		return JSONProblemf(c, http.StatusNotFound, "user %s not found", userId)
	}

	// A UserManager may not remove a Manager, whose privileges exceed theirs, and
	// nobody may remove the last Manager: the collector would have none left.
	isTargetManager, err := odb.UserInRole(ctx, targetID, "Manager")
	if err != nil {
		log.Error("cannot read the teams of the user", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read the user")
	}
	if isTargetManager {
		if !IsManager(c) {
			return JSONProblemf(c, http.StatusForbidden, "only a Manager may delete the account of a Manager")
		}
		n, err := odb.RoleMemberCount(ctx, "Manager")
		if err != nil {
			log.Error("cannot count the Managers", logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot read the user")
		}
		if n <= 1 {
			return JSONProblemf(c, http.StatusConflict, "%s is the last Manager: the collector would have none left", current.Email)
		}
	}

	log.Info("called", "user_id", targetID)
	tx, markSuccess, endTx, err := odb.BeginTxWithControl(ctx, log, &sql.TxOptions{})
	if err != nil {
		log.Error("cannot start transaction", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot delete the user")
	}
	// The transaction ends before the change is announced.
	ended := false
	defer func() {
		if !ended {
			endTx()
		}
	}()
	// The teams go with the account: the log keeps them, read before the removal.
	allRoles, err := tx.UserRoles(ctx, targetID)
	if err != nil {
		log.Error("cannot read the teams of the user", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot delete the user")
	}
	privateRole := "user_" + strconv.FormatInt(targetID, 10)
	roles := make([]string, 0, len(allRoles))
	for _, role := range allRoles {
		if role != privateRole {
			roles = append(roles, role)
		}
	}
	sort.Strings(roles)
	if err := tx.DeleteUserCascade(ctx, targetID); err != nil {
		log.Error("cannot delete user", "user_id", targetID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot delete the user")
	}
	callerEmail, _ := c.Get(XUserEmail).(string)
	name := strings.TrimSpace(current.FirstName + " " + current.LastName)
	logFmt := "deleted user %(email)s"
	if name != "" {
		logFmt += " (%(name)s)"
	}
	if len(roles) > 0 {
		logFmt += ", member of %(groups)s"
	}
	if logErr := tx.Log(ctx, cdb.LogEntry{
		Action: "user.delete",
		User:   callerEmail,
		Fmt:    logFmt,
		Dict: map[string]any{
			"email":  current.Email,
			"name":   name,
			"id":     strconv.FormatInt(targetID, 10),
			"groups": strings.Join(roles, ", "),
		},
		Level: "info",
	}); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
	}
	markSuccess()
	endTx()
	ended = true
	log.Info("user deleted", "user_id", targetID, "email", current.Email, "by", callerEmail)

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}
	return c.JSON(http.StatusOK, map[string]string{"info": "user " + current.Email + " deleted"})
}
