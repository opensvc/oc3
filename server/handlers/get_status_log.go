package serverhandlers

import (
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

const (
	statusLogDefaultDays = 7
	statusLogMaxDays     = 365
)

func statusLogDays(days *int) (int, bool) {
	if days == nil {
		return statusLogDefaultDays, true
	}
	if *days < 1 {
		return 0, false
	}
	return min(*days, statusLogMaxDays), true
}

// GetServiceStatusLog handles GET /services/{svc_id}/status_log: the
// availability periods of a visible service over the last days.
func (a *Api) GetServiceStatusLog(c echo.Context, svcId string, params server.GetServiceStatusLogParams) error {
	log := echolog.GetLogHandler(c, "GetServiceStatusLog")
	days, ok := statusLogDays(params.Days)
	if !ok {
		return JSONProblemf(c, http.StatusBadRequest, "days must be positive")
	}
	if err := a.resolveService(c, log, svcId); err != nil {
		return err
	}
	ctx := c.Request().Context()
	svc, err := a.ODB.ServiceBySvcIDOrName(ctx, svcId)
	if err != nil || svc == nil {
		log.Error("cannot resolve service", "svc_id", svcId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve service %s", svcId)
	}
	periods, err := a.ODB.ServiceStatusPeriods(ctx, svc.SvcID, days)
	if err != nil {
		log.Error("cannot read the status log", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read the status log")
	}
	data := make([]server.ServiceStatusPeriod, 0, len(periods))
	for _, p := range periods {
		data = append(data, server.ServiceStatusPeriod{Status: p.Avail, Begin: p.Begin, End: p.End})
	}
	return c.JSON(http.StatusOK, map[string]any{"data": data})
}

// GetServiceInstanceStatusLog handles GET
// /services/{svc_id}/instances/{node_id}/status_log: the availability and
// overall status periods of an instance of a visible service over the last days.
func (a *Api) GetServiceInstanceStatusLog(c echo.Context, svcId string, nodeId string, params server.GetServiceInstanceStatusLogParams) error {
	log := echolog.GetLogHandler(c, "GetServiceInstanceStatusLog")
	days, ok := statusLogDays(params.Days)
	if !ok {
		return JSONProblemf(c, http.StatusBadRequest, "days must be positive")
	}
	if err := a.resolveService(c, log, svcId); err != nil {
		return err
	}
	ctx := c.Request().Context()
	svc, err := a.ODB.ServiceBySvcIDOrName(ctx, svcId)
	if err != nil || svc == nil {
		log.Error("cannot resolve service", "svc_id", svcId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve service %s", svcId)
	}
	node, err := a.ODB.NodeByNodeIDOrNodename(ctx, nodeId)
	if err != nil {
		log.Error("cannot resolve node", logkey.NodeID, nodeId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve node %s", nodeId)
	}
	if node == nil {
		return JSONProblemf(c, http.StatusNotFound, "node %s not found", nodeId)
	}
	periods, err := a.ODB.InstanceStatusPeriods(ctx, svc.SvcID, node.NodeID, days)
	if err != nil {
		log.Error("cannot read the status log", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot read the status log")
	}
	data := make([]server.InstanceStatusPeriod, 0, len(periods))
	for _, p := range periods {
		data = append(data, server.InstanceStatusPeriod{Avail: p.Avail, Overall: p.Overall, Begin: p.Begin, End: p.End})
	}
	return c.JSON(http.StatusOK, map[string]any{"data": data})
}
