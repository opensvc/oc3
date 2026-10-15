package serverhandlers

import (
	"context"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
)

// GetNodeTargets handles GET /nodes/{node_id}/targets: the array ports the node
// sees through its host bus adapters, with the array owning each, as the
// historical rest_get_node_targets listed them.
func (a *Api) GetNodeTargets(c echo.Context, nodeId string, params server.GetNodeTargetsParams) error {
	log := echolog.GetLogHandler(c, "GetNodeTargets")
	node, err := a.resolveNode(c, log, nodeId)
	if err != nil {
		return err
	}
	return a.handleList(c, "GetNodeTargets", "node_target", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
		filter: params.Filter,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetNodeTargets(ctx, node.NodeID, p)
	})
}
