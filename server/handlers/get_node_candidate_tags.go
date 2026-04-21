package serverhandlers

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// GetNodeCandidateTags handles GET /nodes/{node_id}/candidate_tags
func (a *Api) GetNodeCandidateTags(c echo.Context, nodeId string, params server.GetNodeCandidateTagsParams) error {
	log := echolog.GetLogHandler(c, "GetNodeCandidateTags")
	odb := a.getODB()
	ctx := c.Request().Context()

	node, err := odb.NodeByNodeIDOrNodename(ctx, nodeId)
	if err != nil {
		log.Error("cannot resolve node", logkey.NodeID, nodeId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve node")
	}
	if node == nil {
		return JSONProblemf(c, http.StatusNotFound, "node %s not found", nodeId)
	}

	return a.handleList(c, "GetNodeCandidateTags", "tag", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return odb.GetNodeCandidateTags(ctx, node.NodeID, p)
	})
}
