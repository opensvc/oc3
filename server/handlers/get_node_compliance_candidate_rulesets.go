package serverhandlers

import (
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// GetNodeComplianceCandidateRulesets handles GET /nodes/{node_id}/compliance/candidate_rulesets
func (a *Api) GetNodeComplianceCandidateRulesets(c echo.Context, nodeId string, params server.GetNodeComplianceCandidateRulesetsParams) error {
	page := buildPageParams(params.Limit, params.Offset)
	props, err := buildProps(params.Props, propsMapping["ruleset"])
	if err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}
	log := echolog.GetLogHandler(c, "GetNodeComplianceCandidateRulesets")
	odb := a.getODB()
	ctx := c.Request().Context()

	log.Info("called", logkey.NodeID, nodeId, "limit", page.Limit, "offset", page.Offset, "props", props)

	// get node ID
	node, err := a.getODB().NodeByNodeIDOrNodename(c.Request().Context(), nodeId)
	if err != nil {
		log.Error("cannot find node", "node", nodeId, logkey.Error, err)
		return JSONProblemf(c, http.StatusNotFound, "node %s not found", nodeId)
	}

	// get rulesets already attached to the node
	attachedRulesets, err := odb.CompNodeRulesets(ctx, node.NodeID)
	if err != nil {
		log.Error("cannot get attached rulesets", logkey.NodeID, node.NodeID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get attached rulesets for node %s", node.NodeID)
	}

	// get candidate rulesets
	groups := UserGroupsFromContext(c)
	isManager := IsManager(c)
	candidates, err := odb.CompNodeCandidateRulesets(ctx, node.NodeID, attachedRulesets, groups, isManager, page.Limit, page.Offset)
	if err != nil {
		log.Error("cannot get candidate rulesets", logkey.NodeID, node.NodeID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get candidate rulesets for node %s", node.NodeID)
	}

	filteredItems, err := filterItemsFields(candidates, props)
	if err != nil {
		log.Error("cannot filter ruleset props", logkey.NodeID, node.NodeID, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot filter rulesets fields for node %s", node.NodeID)
	}

	return c.JSON(http.StatusOK, newListResponse(filteredItems, propsMapping["ruleset"], props, page, queryWithMeta(params.Meta), queryWithStats(params.Stats)))

}
