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

// DeleteIp handles DELETE /ips/{id}
func (a *Api) DeleteIp(c echo.Context, id string) error {
	return a.deleteIPByID(c, "DeleteIp", id)
}

func (a *Api) deleteIPByID(c echo.Context, handlerName, id string) error {
	log := echolog.GetLogHandler(c, handlerName)
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}
	if !IsNodeManager(c) {
		return JSONProblemf(c, http.StatusForbidden, "NodeManager privilege required")
	}

	ipID, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return JSONProblemf(c, http.StatusNotFound, "ip %s not found", id)
	}

	ip, found, err := odb.GetNodeIPByID(ctx, ipID)
	if err != nil {
		log.Error("cannot lookup ip", "ip_id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot lookup ip %s", id)
	}
	if !found {
		return JSONProblemf(c, http.StatusNotFound, "ip %s not found", id)
	}

	// Non-managers may only delete ips of nodes they are responsible for.
	// Managers skip the check and may also delete ips whose node no longer exists.
	if !IsManager(c) {
		node, err := odb.NodeByNodeIDOrNodename(ctx, ip.NodeID)
		if err != nil {
			log.Error("cannot lookup node", logkey.NodeID, ip.NodeID, logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot lookup node")
		}
		if node == nil {
			// Python filters the row out via published-nodes: not visible -> not found.
			return JSONProblemf(c, http.StatusNotFound, "ip %s not found", id)
		}
		responsible, err := odb.NodeResponsible(ctx, ip.NodeID, UserGroupsFromContext(c), false)
		if err != nil {
			log.Error("cannot check node responsibility", logkey.NodeID, ip.NodeID, logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot check node responsibility")
		}
		if !responsible {
			return JSONProblemf(c, http.StatusForbidden, "user is not responsible for node %s", ip.NodeID)
		}
	}

	if _, err := odb.DeleteNodeIP(ctx, ipID); err != nil {
		log.Error("cannot delete ip", "ip_id", id, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot delete ip %s", id)
	}

	info := fmt.Sprintf("ip %s on node %s deleted", ip.Addr, ip.NodeID)
	userEmail, _ := c.Get(XUserEmail).(string)
	logEntry := cdb.LogEntry{
		Action: "node.ip.delete",
		User:   userEmail,
		Fmt:    "ip %(addr)s on node %(node_id)s deleted",
		Dict:   map[string]any{"addr": ip.Addr, "node_id": ip.NodeID},
		Level:  "info",
	}
	if parsed, err := uuid.Parse(ip.NodeID); err == nil {
		logEntry.NodeID = &parsed
	}
	if logErr := odb.Log(ctx, logEntry); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
	}

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	return c.JSON(http.StatusOK, map[string]string{"info": info})
}
