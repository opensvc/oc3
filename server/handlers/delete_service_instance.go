package serverhandlers

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// DeleteServiceInstance handles DELETE /services/{svc_id}/instances/{node_id}
func (a *Api) DeleteServiceInstance(c echo.Context, svcId string, nodeId string) error {
	log := echolog.GetLogHandler(c, "DeleteServiceInstance")
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}

	log.Info("called", "svc_id", svcId, logkey.NodeID, nodeId)

	return a.deleteServiceInstanceByKeys(c, log, ctx, svcId, nodeId)
}

// deleteServiceInstanceByKeys deletes the instance of a service
func (a *Api) deleteServiceInstanceByKeys(c echo.Context, log *slog.Logger, ctx context.Context, svcId, nodeId string) error {
	odb := a.ODB

	svc, err := a.resolveServiceRow(c, log, ctx, svcId)
	if err != nil {
		return err
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

	tx, markSuccess, endTx, err := odb.BeginTxWithControl(ctx, log, &sql.TxOptions{})
	if err != nil {
		log.Error("cannot start transaction", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot delete service instance")
	}
	defer endTx()

	info := fmt.Sprintf("delete service %s instance on node %s", svc.Svcname, node.Nodename)
	userEmail, _ := c.Get(XUserEmail).(string)
	if logErr := tx.Log(ctx, cdb.LogEntry{
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

	count, err := tx.DeleteServiceInstanceCascade(ctx, svc.SvcID, node.NodeID)
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
