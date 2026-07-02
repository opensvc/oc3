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

// PostServiceComplianceModuleset handles POST /services/{svc_id}/compliance/modulesets/{mset_id}
func (a *Api) PostServiceComplianceModuleset(c echo.Context, svcId string, msetId string, params server.PostServiceComplianceModulesetParams) error {
	log := echolog.GetLogHandler(c, "PostServiceComplianceModuleset")
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

	// check if the moduleset is already attached to the service
	attached, err := odb.CompModulesetSvcAttached(ctx, svc.SvcID, msetId, slave)
	if err != nil {
		log.Error("cannot check if moduleset is attached", "svc_id", svc.SvcID, logkey.MSetID, msetId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check if moduleset %s is attached to service %s", msetId, svc.SvcID)
	}
	if attached {
		log.Info("moduleset is already attached to this service", "svc_id", svc.SvcID, logkey.MSetID, msetId)
		return JSONProblemf(c, http.StatusConflict, "moduleset %s is already attached to this service", msetId)
	}

	// check if the moduleset is attachable to the service
	attachable, err := odb.CompModulesetSvcAttachable(ctx, svc.SvcID, msetId)
	if err != nil {
		log.Error("cannot check if moduleset is attachable", "svc_id", svc.SvcID, logkey.MSetID, msetId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot check if moduleset %s is attachable to service %s", msetId, svc.SvcID)
	}
	if !attachable {
		log.Info("moduleset is not attachable to this service", "svc_id", svc.SvcID, logkey.MSetID, msetId)
		return JSONProblemf(c, http.StatusForbidden, "moduleset %s is not attachable to this service", msetId)
	}

	// attach moduleset to service
	_, err = odb.CompModulesetAttachService(ctx, svc.SvcID, msetId, slave)
	if err != nil {
		log.Error("cannot attach moduleset to service", "svc_id", svc.SvcID, logkey.MSetID, msetId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot attach moduleset %s to service %s", msetId, svc.SvcID)
	}

	response := map[string]string{
		"info": fmt.Sprintf("moduleset %s attached to service %s", msetId, svc.SvcID),
	}

	return c.JSON(http.StatusAccepted, response)
}
