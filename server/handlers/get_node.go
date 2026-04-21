package serverhandlers

import (
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// GetNode handles GET /nodes/{node_id}
func (a *Api) GetNode(c echo.Context, nodeId string, params server.GetNodeParams) error {
	query, err := buildListQueryParameters(params.Props, params.Limit, params.Offset, params.Meta, params.Stats, params.Orderby, params.Groupby, propsMapping["node"])
	if err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	log := echolog.GetLogHandler(c, "GetNode")
	odb := a.getODB()
	ctx := c.Request().Context()
	groups := UserGroupsFromContext(c)
	isManager := IsManager(c)

	log.Info("called", "node_id", nodeId, "props", query.Props, "is_manager", isManager)

	selectExprs, err := buildSelectClause(query.Props, propsMapping["node"])
	if err != nil {
		log.Error("cannot build select clause", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot build select clause")
	}

	nodes, err := odb.GetNode(ctx, nodeId, cdb.ListParams{
		Groups:      groups,
		IsManager:   isManager,
		Limit:       query.Page.Limit,
		Offset:      query.Page.Offset,
		Props:       query.Props,
		SelectExprs: selectExprs,
	})
	if err != nil {
		log.Error("cannot get node", "node_id", nodeId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get node")
	}
	if len(nodes) == 0 {
		return JSONProblemf(c, http.StatusNotFound, "node %s not found", nodeId)
	}

	return c.JSON(http.StatusOK, newListResponse(nodes, propsMapping["node"], query))
}
