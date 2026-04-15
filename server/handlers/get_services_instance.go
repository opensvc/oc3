package serverhandlers

import (
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// GetServicesInstance handles GET /services_instances/{svc_id}
func (a *Api) GetServicesInstance(c echo.Context, svcId string, params server.GetServicesInstanceParams) error {
	query, err := buildListQueryParameters(params.Props, params.Limit, params.Offset, params.Meta, params.Stats, params.Orderby, params.Groupby, propsMapping["instance"])
	if err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	log := echolog.GetLogHandler(c, "GetServicesInstance")
	odb := a.getODB()
	ctx := c.Request().Context()
	groups := UserGroupsFromContext(c)
	isManager := IsManager(c)

	log.Info("called", "svc_id", svcId, "props", query.Props, "is_manager", isManager)

	selectExprs, err := buildSelectClause(query.Props, propsMapping["instance"])
	if err != nil {
		log.Error("cannot build select clause", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot build select clause")
	}

	instances, err := odb.GetServicesInstance(ctx, svcId, cdb.ListParams{
		Groups:      groups,
		IsManager:   isManager,
		Limit:       query.Page.Limit,
		Offset:      query.Page.Offset,
		Props:       query.Props,
		SelectExprs: selectExprs,
	})
	if err != nil {
		log.Error("cannot get service instances", "svc_id", svcId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get service instances")
	}
	if len(instances) == 0 {
		return JSONProblemf(c, http.StatusNotFound, "service %s not found or has no instances", svcId)
	}

	return c.JSON(http.StatusOK, newListResponse(instances, propsMapping["instance"], query))
}
