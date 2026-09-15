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

// GetNodeComplianceLogs handles GET /nodes/{node_id}/compliance/logs
func (a *Api) GetNodeComplianceLogs(c echo.Context, nodeId server.InPathNodeId, params server.GetNodeComplianceLogsParams) error {
	log := echolog.GetLogHandler(c, "GetNodeComplianceLogs")
	ctx := c.Request().Context()

	node, err := a.ODB.NodeByNodeIDOrNodename(ctx, string(nodeId))
	if err != nil {
		log.Error("cannot resolve node", logkey.NodeID, nodeId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve node %s", nodeId)
	}
	if node == nil {
		return JSONProblemf(c, http.StatusNotFound, "node %s not found", nodeId)
	}

	return a.handleList(c, "GetNodeComplianceLogs", "comp_log", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetNodeComplianceLogs(ctx, node.NodeID, p)
	})
}
