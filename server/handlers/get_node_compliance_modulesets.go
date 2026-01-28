package serverhandlers

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

// GetNodeComplianceModulesets handles GET /nodes/{node_id}/compliance/modulesets
func (a *Api) GetNodeComplianceModulesets(c echo.Context, nodeId string) error {
	log := getLog(c)
	odb := a.cdbSession()
	ctx := c.Request().Context()

	log.Info("GetNodeComplianceModulesets called", "node_id", nodeId)

	// get node ID
	node, err := odb.NodeByNodeIDOrNodename(ctx, nodeId)
	if err != nil {
		log.Error("GetNodeComplianceModulesets: cannot find node", "node", nodeId, "error", err)
		return JSONProblemf(c, http.StatusNotFound, "NotFound", "node %s not found", nodeId)
	}

	// get attached modulesets with details
	groups := UserGroupsFromContext(c)
	isManager := IsManager(c)
	modulesets, err := odb.CompNodeAttachedModulesets(ctx, node.NodeID, groups, isManager)
	if err != nil {
		log.Error("GetNodeComplianceModulesets: cannot get attached modulesets", "node_id", node.NodeID, "error", err)
		return JSONProblemf(c, http.StatusInternalServerError, "InternalError", "cannot get attached modulesets for node %s", node.NodeID)
	}

	return c.JSON(http.StatusOK, modulesets)
}
