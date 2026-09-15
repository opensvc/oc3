package serverhandlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// PostActions handles POST /actions.
func (a *Api) PostActions(c echo.Context) error {
	log := echolog.GetLogHandler(c, "PostActions")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}

	var body map[string]any
	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	rawID, ok := body["id"]
	if !ok {
		return JSONProblemf(c, http.StatusBadRequest, "The 'id' key must be specified")
	}
	idStr := fmt.Sprintf("%v", rawID)

	if f, err := strconv.ParseFloat(idStr, 64); err == nil {
		idStr = strconv.FormatInt(int64(f), 10)
	}
	actionID, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		return JSONProblemf(c, http.StatusBadRequest, "invalid action id %q", idStr)
	}

	if !(IsManager(c) || HasGroup(c, "NodeExec") || HasGroup(c, "CompExec")) {
		return JSONProblemf(c, http.StatusForbidden, "user has no NodeExec, CompExec privilege")
	}

	action, found, err := odb.GetActionByID(ctx, actionID)
	if err != nil {
		log.Error("cannot lookup action", "action_id", actionID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot lookup action %d", actionID)
	}
	if !found {
		return c.JSON(http.StatusOK, map[string]string{
			"error": fmt.Sprintf("Action %d does not exist in action queue", actionID),
		})
	}

	if action.NodeID == "" {
		return JSONProblemf(c, http.StatusBadRequest, "node_responsible() must have a not None node_id parameter")
	}
	if !IsManager(c) {
		node, err := odb.NodeByNodeIDOrNodename(ctx, action.NodeID)
		if err != nil {
			log.Error("cannot lookup node", logkey.NodeID, action.NodeID, logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot lookup node")
		}
		if node == nil {
			return JSONProblemf(c, http.StatusNotFound, "Node %s does not exist", action.NodeID)
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

	// Only 'status' is updatable. Any other property is rejected.
	var invalid []string
	for k := range body {
		if k != "id" && k != "status" {
			invalid = append(invalid, k)
		}
	}
	_, hasStatus := body["status"]
	if len(invalid) > 0 || !hasStatus {
		sort.Strings(invalid)
		return c.JSON(http.StatusOK, map[string]string{
			"error": fmt.Sprintf("Permission denied: properties not updateable: %s", strings.Join(invalid, ", ")),
		})
	}

	newStatus, _ := body["status"].(string)

	if action.Status == "T" && newStatus == "C" {
		return c.JSON(http.StatusOK, map[string]string{
			"error": fmt.Sprintf("Can not cancel action %d in %s state", action.ID, action.Status),
		})
	}
	resetDequeued := false
	if newStatus == "W" {
		if action.Status == "R" || action.Status == "W" {
			return c.JSON(http.StatusOK, map[string]string{
				"error": fmt.Sprintf("Can not redo action %d in %s state", action.ID, action.Status),
			})
		}
		resetDequeued = true
	}

	if err := odb.UpdateActionStatus(ctx, action.ID, newStatus, resetDequeued); err != nil {
		log.Error("cannot update action", "action_id", action.ID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot update action %d", action.ID)
	}

	userEmail, _ := c.Get(XUserEmail).(string)
	logEntry := cdb.LogEntry{
		Action: "action_queue.update",
		User:   userEmail,
		Fmt:    "update properties %(data)s",
		Dict:   map[string]any{"data": fmt.Sprintf("status: %s => %s", action.Status, newStatus)},
		Level:  "info",
	}
	if parsed, err := uuid.Parse(action.NodeID); err == nil {
		logEntry.NodeID = &parsed
	}
	if action.SvcID != "" {
		if parsed, err := uuid.Parse(action.SvcID); err == nil {
			logEntry.SvcID = &parsed
		}
	}
	if logErr := odb.Log(ctx, logEntry); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
	}

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	return fetchAndReturnAction(c, odb, ctx, action.ID, "cannot fetch updated action")
}

func fetchAndReturnAction(c echo.Context, odb *cdb.DB, ctx context.Context, id int64, errMsg string) error {
	mapping := propsMapping["action_queue"]
	props := defaultProps(mapping)
	selectExprs, err := buildSelectClause(props, mapping)
	if err != nil {
		return JSONProblemf(c, http.StatusInternalServerError, "%s", errMsg)
	}
	rows, err := odb.GetActionByIDMapped(ctx, id, cdb.ListParams{
		Props:       props,
		SelectExprs: selectExprs,
		TypeHints:   buildTypeHints(props, mapping),
	})
	if err != nil || len(rows) == 0 {
		return JSONProblemf(c, http.StatusInternalServerError, "%s", errMsg)
	}
	return c.JSON(http.StatusOK, rows[0])
}
