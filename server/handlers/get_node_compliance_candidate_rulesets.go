package serverhandlers

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

// GetNodeComplianceCandidateRulesets handles GET /nodes/{node_id}/compliance/candidate_rulesets
func (a *Api) GetNodeComplianceCandidateRulesets(c echo.Context, nodeId string) error {
	log := getLog(c)
	odb := a.cdbSession()
	ctx := c.Request().Context()

	log.Info("GetNodeComplianceCandidateRulesets called", "node_id", nodeId)

	// get node ID
	node, err := a.cdbSession().NodeByNodeIDOrNodename(c.Request().Context(), nodeId)
	if err != nil {
		log.Error("GetNodeComplianceCandidateRulesets: cannot find node", "node", nodeId, "error", err)
		return JSONProblemf(c, http.StatusNotFound, "node %s not found", nodeId)
	}

	// get rulesets already attached to the node
	attachedRulesets, err := odb.CompNodeRulesets(ctx, node.NodeID)
	if err != nil {
		log.Error("GetNodeComplianceCandidateRulesets: cannot get attached rulesets", "node_id", node.NodeID, "error", err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get attached rulesets for node %s", node.NodeID)
	}

	// get candidate rulesets
	groups := UserGroupsFromContext(c)
	isManager := IsManager(c)
	candidates, err := odb.CompNodeCandidateRulesets(ctx, node.NodeID, attachedRulesets, groups, isManager)
	if err != nil {
		log.Error("GetNodeComplianceCandidateRulesets: cannot get candidate rulesets", "node_id", node.NodeID, "error", err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get candidate rulesets for node %s", node.NodeID)
	}

	return c.JSON(http.StatusOK, candidates)

}
