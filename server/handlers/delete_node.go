package serverhandlers

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// DeleteNode handles DELETE /nodes/{node_id}
func (a *Api) DeleteNode(c echo.Context, nodeId string) error {
	return a.deleteNodeByID(c, "DeleteNode", nodeId)
}

func (a *Api) deleteNodeByID(c echo.Context, handlerName, nodeId string) error {
	log := echolog.GetLogHandler(c, handlerName)
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}
	if !IsManager(c) {
		return JSONProblemf(c, http.StatusForbidden, "NodeManager privilege required")
	}

	log.Info("called", logkey.NodeID, nodeId)

	node, err := odb.NodeByNodeIDOrNodename(ctx, nodeId)
	if err != nil {
		log.Error("cannot lookup node", logkey.NodeID, nodeId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot lookup node %s", nodeId)
	}
	if node == nil {
		return JSONProblemf(c, http.StatusNotFound, "node %s not found", nodeId)
	}

	responsible, err := odb.NodeResponsible(ctx, node.NodeID, UserGroupsFromContext(c), false)
	if err != nil {
		log.Error("cannot check node responsibility", logkey.NodeID, node.NodeID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check node responsibility")
	}
	if !responsible {
		return JSONProblemf(c, http.StatusForbidden, "you are not responsible for node %s", node.Nodename)
	}

	markSuccess, endTx, err := odb.BeginTxWithControl(ctx, log, &sql.TxOptions{})
	if err != nil {
		log.Error("cannot start transaction", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot delete node")
	}
	defer endTx()

	if err := odb.DeleteNodeCascade(ctx, node.NodeID); err != nil {
		log.Error("cannot delete node", logkey.NodeID, node.NodeID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot delete node %s", node.Nodename)
	}

	msg := fmt.Sprintf("delete node %s", node.Nodename)
	userEmail, _ := c.Get(XUserEmail).(string)
	logEntry := cdb.LogEntry{
		Action: "node.delete",
		User:   userEmail,
		Fmt:    "delete node %(data)s",
		Dict:   map[string]any{"data": node.Nodename},
		Level:  "info",
	}
	if parsed, err := uuid.Parse(node.NodeID); err == nil {
		logEntry.NodeID = &parsed
	}
	if logErr := odb.Log(ctx, logEntry); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot write audit log")
	}

	markSuccess()

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	return c.JSON(http.StatusOK, map[string]string{"info": msg})
}
