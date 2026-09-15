package serverhandlers

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// DeleteAction handles DELETE /actions/{id}
func (a *Api) DeleteAction(c echo.Context, id string) error {
	log := echolog.GetLogHandler(c, "DeleteAction")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}
	if !IsNodeManager(c) {
		return JSONProblemf(c, http.StatusForbidden, "NodeManager privilege required")
	}

	notExist := func() error {
		return c.JSON(http.StatusOK, map[string]string{
			"info": fmt.Sprintf("Action %s does not exist in action queue", id),
		})
	}

	actionID, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return notExist()
	}

	action, found, err := odb.GetActionByID(ctx, actionID)
	if err != nil {
		log.Error("cannot lookup action", "action_id", actionID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot lookup action %d", actionID)
	}
	if !found {
		return notExist()
	}

	// Non-managers may only delete actions of nodes they are responsible for.
	if !IsManager(c) {
		if action.NodeID == "" {
			return JSONProblemf(c, http.StatusBadRequest, "node_responsible() must have a not None node_id parameter")
		}
		node, err := odb.NodeByNodeIDOrNodename(ctx, action.NodeID)
		if err != nil {
			log.Error("cannot lookup node", logkey.NodeID, action.NodeID, logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot lookup node")
		}
		if node == nil {
			return notExist()
		}
		responsible, err := odb.NodeResponsible(ctx, action.NodeID, UserGroupsFromContext(c), false)
		if err != nil {
			log.Error("cannot check node responsibility", logkey.NodeID, action.NodeID, logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot check node responsibility")
		}
		if !responsible {
			return JSONProblemf(c, http.StatusForbidden, "user is not responsible for node %s", action.NodeID)
		}
	}

	if _, err := odb.DeleteAction(ctx, actionID); err != nil {
		log.Error("cannot delete action", "action_id", actionID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot delete action %d", actionID)
	}

	userEmail, _ := c.Get(XUserEmail).(string)
	logEntry := cdb.LogEntry{
		Action: "action_queue.delete",
		User:   userEmail,
		Fmt:    "deleted actions %(u)s",
		Dict:   map[string]any{"u": action.Command},
		Level:  "info",
	}
	if parsed, err := uuid.Parse(action.NodeID); err == nil {
		logEntry.NodeID = &parsed
	}
	if logErr := odb.Log(ctx, logEntry); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
	}

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	return c.JSON(http.StatusOK, map[string]string{
		"info": fmt.Sprintf("Action %s deleted", id),
	})
}
