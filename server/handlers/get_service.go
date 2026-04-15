package serverhandlers

import (
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// GetService handles GET /services/{svc_id}
func (a *Api) GetService(c echo.Context, svcId string, params server.GetServiceParams) error {
	query, err := buildListQueryParameters(params.Props, params.Limit, params.Offset, params.Meta, params.Stats, params.Orderby, params.Groupby, propsMapping["service"])
	if err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	log := echolog.GetLogHandler(c, "GetService")
	odb := a.getODB()
	ctx := c.Request().Context()
	groups := UserGroupsFromContext(c)
	isManager := IsManager(c)

	log.Info("called", "svc_id", svcId, "props", query.Props, "is_manager", isManager)

	selectExprs, err := buildSelectClause(query.Props, propsMapping["service"])
	if err != nil {
		log.Error("cannot build select clause", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot build select clause")
	}

	services, err := odb.GetService(ctx, svcId, cdb.ListParams{
		Groups:      groups,
		IsManager:   isManager,
		Limit:       query.Page.Limit,
		Offset:      query.Page.Offset,
		Props:       query.Props,
		SelectExprs: selectExprs,
	})
	if err != nil {
		log.Error("cannot get service", "svc_id", svcId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get service")
	}
	if len(services) == 0 {
		return JSONProblemf(c, http.StatusNotFound, "service %s not found", svcId)
	}

	return c.JSON(http.StatusOK, newListResponse(services, propsMapping["service"], query))
}
