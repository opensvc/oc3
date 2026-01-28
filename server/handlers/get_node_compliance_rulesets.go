package serverhandlers

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

// GetNodeComplianceRulesets handles GET /nodes/{node_id}/compliance/rulesets
func (a *Api) GetNodeComplianceRulesets(c echo.Context, nodeId string) error {
	log := getLog(c)
	odb := a.cdbSession()
	ctx := c.Request().Context()

	log.Info("GetNodeComplianceRulesets called", "node_id", nodeId)

	// get node ID
	node, err := odb.NodeByNodeIDOrNodename(ctx, nodeId)
	if err != nil {
		log.Error("GetNodeComplianceRulesets: cannot find node", "node", nodeId, "error", err)
		return JSONProblemf(c, http.StatusNotFound, "NotFound", "node %s not found", nodeId)
	}

	// get attached rulesets with details
	groups := UserGroupsFromContext(c)
	isManager := IsManager(c)
	rulesets, err := odb.CompNodeAttachedRulesets(ctx, node.NodeID, groups, isManager)
	if err != nil {
		log.Error("GetNodeComplianceRulesets: cannot get attached rulesets", "node_id", node.NodeID, "error", err)
		return JSONProblemf(c, http.StatusInternalServerError, "InternalError", "cannot get attached rulesets for node %s", node.NodeID)
	}

	return c.JSON(http.StatusOK, rulesets)
}
