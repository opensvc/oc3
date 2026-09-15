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

// PostGroupHiddenMenuEntries handles POST /groups/{group_id}/hidden_menu_entries
func (a *Api) PostGroupHiddenMenuEntries(c echo.Context, groupId string) error {
	log := echolog.GetLogHandler(c, "PostGroupHiddenMenuEntries")
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

	var body server.PostGroupHiddenMenuEntriesJSONRequestBody
	if err := c.Bind(&body); err != nil {
		log.Error("invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	menuEntry := body.MenuEntry
	if menuEntry == "" {
		return JSONProblemf(c, http.StatusBadRequest, "'menu_entry' key must be set")
	}
	if _, ok := validMenuEntries[menuEntry]; !ok {
		return JSONProblemf(c, http.StatusBadRequest, "invalid menu entry %s", menuEntry)
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

	exists, err := odb.GroupHiddenMenuEntryExists(ctx, group.ID, menuEntry)
	if err != nil {
		log.Error("cannot check hidden menu entry", "group_id", groupId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check hidden menu entry")
	}
	if exists {
		return c.JSON(http.StatusOK, map[string]string{
			"info": fmt.Sprintf("menu entry %s is already hidden for group %s", menuEntry, group.Role),
		})
	}

	if err := odb.InsertGroupHiddenMenuEntry(ctx, group.ID, menuEntry); err != nil {
		log.Error("cannot hide menu entry", "group_id", groupId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot hide menu entry %s for group %s", menuEntry, group.Role)
	}

	userEmail, _ := c.Get(XUserEmail).(string)
	if logErr := odb.Log(ctx, cdb.LogEntry{
		Action: "groups.hidden_menu_entries.add",
		User:   userEmail,
		Fmt:    "hide %(m)s for group %(g)s",
		Dict:   map[string]any{"m": menuEntry, "g": group.Role},
		Level:  "info",
	}); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
	}

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	return c.JSON(http.StatusOK, map[string]string{
		"info": fmt.Sprintf("menu entry %s hidden for group %s", menuEntry, group.Role),
	})
}
