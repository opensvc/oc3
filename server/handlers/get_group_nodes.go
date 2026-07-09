package serverhandlers

import (
	"context"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

// GetGroupNodes handles GET /groups/{group_id}/nodes
func (a *Api) GetGroupNodes(c echo.Context, groupId string, params server.GetGroupNodesParams) error {
	return a.handleList(c, "GetGroupNodes", "node", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetGroupNodes(ctx, groupId, p)
	})
}
