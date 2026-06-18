package serverhandlers

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// DeleteServiceInstance handles DELETE /services/{svc_id}/instances/{node_id}
func (a *Api) DeleteServiceInstance(c echo.Context, svcId string, nodeId string) error {
	log := echolog.GetLogHandler(c, "DeleteServiceInstance")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}

	svc, err := odb.ServiceBySvcIDOrName(ctx, svcId)
	if err != nil {
		log.Error("cannot lookup service", "svc_id", svcId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot lookup service %s", svcId)
	}
	if svc == nil {
		return JSONProblemf(c, http.StatusNotFound, "service %s not found", svcId)
	}

	node, err := odb.NodeByNodeIDOrNodename(ctx, nodeId)
	if err != nil {
		log.Error("cannot lookup node", logkey.NodeID, nodeId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot lookup node %s", nodeId)
	}
	if node == nil {
		return JSONProblemf(c, http.StatusNotFound, "node %s not found", nodeId)
	}

	responsible, err := odb.ServiceResponsible(ctx, svc.SvcID, UserGroupsFromContext(c), IsManager(c))
	if err != nil {
		log.Error("cannot check service responsibility", "svc_id", svc.SvcID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check service responsibility")
	}
	if !responsible {
		return JSONProblemf(c, http.StatusForbidden, "you are not responsible for service %s", svc.Svcname)
	}

	markSuccess, endTx, err := odb.BeginTxWithControl(ctx, log, &sql.TxOptions{})
	if err != nil {
		log.Error("cannot start transaction", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot delete service instance")
	}
	defer endTx()

	info := fmt.Sprintf("delete service %s instance on node %s", svc.Svcname, node.Nodename)
	userEmail, _ := c.Get(XUserEmail).(string)
	if logErr := odb.Log(ctx, cdb.LogEntry{
		Action: "service_instance.delete",
		User:   userEmail,
		Fmt:    "delete service %(svcname)s instance on node %(nodename)s",
		Dict: map[string]any{
			"svcname":  svc.Svcname,
			"nodename": node.Nodename,
		},
		Level: "info",
	}); logErr != nil {
		log.Error("cannot write audit log", logkey.Error, logErr)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot write audit log")
	}

	count, err := odb.DeleteServiceInstanceCascade(ctx, svc.SvcID, node.NodeID)
	if err != nil {
		log.Error("cannot delete service instance", "svc_id", svc.SvcID, logkey.NodeID, node.NodeID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot delete service instance")
	}
	if count == 0 {
		return JSONProblemf(c, http.StatusNotFound, "service instance %s on node %s does not exist", svc.Svcname, node.Nodename)
	}

	markSuccess()

	if err := odb.Session.NotifyChanges(ctx); err != nil {
		log.Error("cannot notify changes", logkey.Error, err)
	}

	return c.JSON(http.StatusOK, map[string]string{"info": info})
}
