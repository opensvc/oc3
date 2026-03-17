package serverhandlers

import (
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// GetNodeComplianceModulesets handles GET /nodes/{node_id}/compliance/modulesets
func (a *Api) GetNodeComplianceModulesets(c echo.Context, nodeId string, params server.GetNodeComplianceModulesetsParams) error {
	page := buildPageParams(params.Limit, params.Offset)
	log := echolog.GetLogHandler(c, "GetNodeComplianceModulesets")
	odb := a.getODB()
	ctx := c.Request().Context()

	log.Info("called", logkey.NodeID, nodeId, "limit", page.Limit, "offset", page.Offset)

	// get node ID
	node, err := odb.NodeByNodeIDOrNodename(ctx, nodeId)
	if err != nil {
		log.Error("cannot find node", "node", nodeId, logkey.Error, err)
		return JSONProblemf(c, http.StatusNotFound, "node %s not found", nodeId)
	}

	// get attached modulesets with details
	groups := UserGroupsFromContext(c)
	isManager := IsManager(c)
	modulesets, err := odb.CompNodeAttachedModulesets(ctx, node.NodeID, groups, isManager, page.Limit, page.Offset)
	if err != nil {
		log.Error("cannot get attached modulesets", logkey.NodeID, node.NodeID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get attached modulesets for node %s", node.NodeID)
	}

	return c.JSON(http.StatusOK, modulesets)
}
