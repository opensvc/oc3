package serverhandlers

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// DeleteServicesInstances handles DELETE /services_instances
func (a *Api) DeleteServicesInstances(c echo.Context) error {
	log := echolog.GetLogHandler(c, "DeleteServicesInstances")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	if !IsAuthByUser(c) {
		return JSONProblemf(c, http.StatusUnauthorized, "user authentication required")
	}

	var body server.DeleteServicesInstancesJSONRequestBody
	if err := c.Bind(&body); err != nil {
		log.Error("invalid request body", logkey.Error, err)
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	var svcId, nodeId string
	switch {
	case body.Id != nil:
		id := *body.Id
		resolvedSvcId, resolvedNodeId, err := odb.ServiceInstanceByID(ctx, id)
		if err != nil {
			log.Error("cannot lookup service instance", "id", id, logkey.Error, err)
			return JSONProblemf(c, http.StatusInternalServerError, "cannot lookup service instance %d", id)
		}
		if resolvedSvcId == "" || resolvedNodeId == "" {
			return JSONProblemf(c, http.StatusNotFound, "service instance %d does not exist", id)
		}
		svcId, nodeId = resolvedSvcId, resolvedNodeId
	case body.SvcId != nil && body.NodeId != nil:
		svcId, nodeId = *body.SvcId, *body.NodeId
	default:
		return JSONProblemf(c, http.StatusBadRequest, "'svc_id+node_id' or 'id' keys must be specified")
	}

	log.Info("called", "svc_id", svcId, logkey.NodeID, nodeId)

	return a.deleteServiceInstanceByKeys(c, log, ctx, svcId, nodeId)
}
