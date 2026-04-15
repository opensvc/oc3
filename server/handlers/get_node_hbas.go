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

// GetNodeHbas handles GET /nodes/{node_id}/hbas
func (a *Api) GetNodeHbas(c echo.Context, nodeId string, params server.GetNodeHbasParams) error {
	log := echolog.GetLogHandler(c, "GetNodeHbas")
	odb := a.getODB()
	ctx := c.Request().Context()

	node, err := odb.NodeByNodeIDOrNodename(ctx, nodeId)
	if err != nil {
		log.Error("cannot resolve node", "node_id", nodeId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve node")
	}
	if node == nil {
		return JSONProblemf(c, http.StatusNotFound, "node %s not found", nodeId)
	}

	return a.handleList(c, "GetNodeHbas", "hba", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return odb.GetNodeHbas(ctx, node.NodeID, p)
	})
}
