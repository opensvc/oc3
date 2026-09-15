package serverhandlers

import (
	"context"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// DeleteGroupHiddenMenuEntries handles DELETE /groups/{group_id}/hidden_menu_entries
func (a *Api) DeleteGroupHiddenMenuEntries(c echo.Context, groupId string) error {
	log := echolog.GetLogHandler(c, "DeleteGroupHiddenMenuEntries")
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

	var body server.DeleteGroupHiddenMenuEntriesJSONRequestBody
	if err := c.Bind(&body); err != nil {
		log.Error("invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	menuEntry := body.MenuEntry
	if menuEntry == "" {
		return JSONProblemf(c, http.StatusBadRequest, "'menu_entry' key must be set")
	}

	isManager := IsManager(c)
	userGroupIDs, err := a.resolveUserGroupIDs(c, log)
	if err != nil {
		return err
	}

	group, status, err := odb.OrgGroup(ctx, groupId, userGroupIDs, isManager)
	if err != nil {
		log.Error("cannot resolve group", "group_id", groupId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve group %s", groupId)
	}
	switch status {
	case cdb.OrgGroupNotFound:
		return c.JSON(http.StatusOK, map[string]string{
			"info": fmt.Sprintf("Group %s does not exists", groupId),
		})
	case cdb.OrgGroupAmbiguous:
		return JSONProblemf(c, http.StatusBadRequest, "ambiguous group id: %s", groupId)
	case cdb.OrgGroupPrivileged:
		return JSONProblemf(c, http.StatusBadRequest, "Can not set hidden menu entries for privilege groups")
	}
	if !isManager && group.Role == "Everybody" {
		return JSONProblemf(c, http.StatusBadRequest, "The 'Everybody' group is immutable")
	}

	n, err := odb.DeleteGroupHiddenMenuEntry(ctx, group.ID, menuEntry)
	if err != nil {
		log.Error("cannot unhide menu entry", "group_id", groupId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot unhide menu entry %s for group %s", menuEntry, group.Role)
	}
	if n == 0 {
		return c.JSON(http.StatusOK, map[string]string{
			"info": fmt.Sprintf("menu entry %s is already not hidden for group %s", menuEntry, group.Role),
		})
	}

	userEmail, _ := c.Get(XUserEmail).(string)
	if logErr := odb.Log(ctx, cdb.LogEntry{
		Action: "groups.hidden_menu_entries.delete",
		User:   userEmail,
		Fmt:    "unhide %(m)s for group %(g)s",
		Dict:   map[string]any{"m": menuEntry, "g": group.Role},
		Level:  "info",
	}); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
	}

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	return c.JSON(http.StatusOK, map[string]string{
		"info": fmt.Sprintf("menu entry %s unhidden for group %s", menuEntry, group.Role),
	})
}
