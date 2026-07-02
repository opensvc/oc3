package serverhandlers

import (
	"context"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// DeleteServiceComplianceModuleset handles DELETE /services/{svc_id}/compliance/modulesets/{mset_id}
func (a *Api) DeleteServiceComplianceModuleset(c echo.Context, svcId string, msetId string, params server.DeleteServiceComplianceModulesetParams) error {
	log := echolog.GetLogHandler(c, "DeleteServiceComplianceModuleset")
	odb := a.ODB
	ctx, cancel := context.WithTimeout(c.Request().Context(), a.SyncTimeout)
	defer cancel()

	slave := false
	if params.Slave != nil {
		slave = *params.Slave
	}

	log.Info("called", "svc_id", svcId, logkey.MSetID, msetId, "slave", slave)

	svc, err := odb.ServiceBySvcIDOrName(ctx, svcId)
	if err != nil {
		log.Error("cannot resolve service", "svc_id", svcId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve service %s", svcId)
	}
	if svc == nil {
		return JSONProblemf(c, http.StatusNotFound, "service %s not found", svcId)
	}

	responsible, err := odb.ServiceResponsible(ctx, svc.SvcID, UserGroupsFromContext(c), IsManager(c))
	if err != nil {
		log.Error("cannot check if user is responsible for the service", "svc_id", svc.SvcID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check if user is responsible for service %s", svc.SvcID)
	}
	if !responsible {
		log.Info("user is not responsible for this service", "svc_id", svc.SvcID)
		return JSONProblemf(c, http.StatusForbidden, "user is not responsible for service %s", svc.SvcID)
	}

	_, err = odb.CompModulesetName(ctx, msetId)
	if err != nil {
		log.Error("cannot find moduleset", logkey.MSetID, msetId, logkey.Error, err)
		return JSONProblemf(c, http.StatusNotFound, "moduleset %s not found", msetId)
	}

	// check if the moduleset is attached to the service
	attached, err := odb.CompModulesetSvcAttached(ctx, svc.SvcID, msetId, slave)
	if err != nil {
		log.Error("cannot check if moduleset is attached", "svc_id", svc.SvcID, logkey.MSetID, msetId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check if moduleset %s is attached to service %s", msetId, svc.SvcID)
	}
	if !attached {
		log.Info("moduleset is not attached to this service", "svc_id", svc.SvcID, logkey.MSetID, msetId)
		return JSONProblemf(c, http.StatusConflict, "moduleset %s is not attached to this service", msetId)
	}

	// detach moduleset from service
	_, err = odb.CompModulesetDetachService(ctx, svc.SvcID, msetId, slave)
	if err != nil {
		log.Error("cannot detach moduleset from service", "svc_id", svc.SvcID, logkey.MSetID, msetId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot detach moduleset %s from service %s", msetId, svc.SvcID)
	}

	response := map[string]string{
		"info": fmt.Sprintf("moduleset %s detached from service %s", msetId, svc.SvcID),
	}

	return c.JSON(http.StatusAccepted, response)
}
