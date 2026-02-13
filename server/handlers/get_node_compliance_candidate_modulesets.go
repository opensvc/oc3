package serverhandlers

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

// GetNodeComplianceCandidateModulesets handles GET /nodes/{node_id}/compliance/candidate_modulesets
func (a *Api) GetNodeComplianceCandidateModulesets(c echo.Context, nodeId string) error {
	log := getLog(c)
	odb := a.cdbSession()
	ctx := c.Request().Context()

	log.Info("GetNodeComplianceCandidateModulesets called", "node_id", nodeId)

	// get node ID
	node, err := odb.NodeByNodeIDOrNodename(ctx, nodeId)
	if err != nil {
		log.Error("GetNodeComplianceCandidateModulesets: cannot find node", "node", nodeId, "error", err)
		return JSONProblemf(c, http.StatusNotFound, "node %s not found", nodeId)
	}

	// get modulesets already attached to the node
	attachedModulesets, err := odb.CompNodeModulesets(ctx, node.NodeID)
	if err != nil {
		log.Error("GetNodeComplianceCandidateModulesets: cannot get attached modulesets", "node_id", node.NodeID, "error", err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get attached modulesets for node %s", node.NodeID)
	}

	// get candidate modulesets
	groups := UserGroupsFromContext(c)
	isManager := IsManager(c)
	candidates, err := odb.CompNodeCandidateModulesets(ctx, node.NodeID, attachedModulesets, groups, isManager)
	if err != nil {
		log.Error("GetNodeComplianceCandidateModulesets: cannot get candidate modulesets", "node_id", node.NodeID, "error", err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get candidate modulesets for node %s", node.NodeID)
	}

	return c.JSON(http.StatusOK, candidates)
}
