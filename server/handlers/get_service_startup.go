package serverhandlers

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// GetServiceStartup handles GET /services/{svc_id}/startup, as the historical
// collector's json_startup_data.
func (a *Api) GetServiceStartup(c echo.Context, svcId string, params server.GetServiceStartupParams) error {
	log := echolog.GetLogHandler(c, "GetServiceStartup")
	ctx := c.Request().Context()
	groups, isManager := UserGroupsFromContext(c), IsManager(c)

	svc, err := a.ODB.ServiceBySvcIDOrName(ctx, svcId, groups, isManager)
	if errors.Is(err, cdb.ErrServiceAmbiguous) {
		return JSONProblemf(c, http.StatusConflict, "%s", err)
	}
	if err != nil {
		log.Error("cannot resolve service", "svc_id", svcId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve service %s", svcId)
	}
	if svc == nil {
		return JSONProblemf(c, http.StatusNotFound, "service %s not found", svcId)
	}
	r := cdb.StartupRequest{SvcID: svc.SvcID, Groups: groups, IsManager: isManager}
	if params.NodeId != nil {
		r.NodeID = *params.NodeId
	}
	if params.ShowDisabled != nil {
		r.ShowDisabled = *params.ShowDisabled
	}
	s, err := a.ODB.GetServiceStartup(ctx, r)
	if errors.Is(err, cdb.ErrStartupNodeUnknown) {
		return JSONProblemf(c, http.StatusBadRequest, "node %s: %s", r.NodeID, err)
	}
	if err != nil {
		return httpProblem(c, httpInternal(log, "cannot build the startup graph", err))
	}
	if s == nil {
		return JSONProblemf(c, http.StatusNotFound, "service %s not found", svcId)
	}
	return c.JSON(http.StatusOK, s)
}
