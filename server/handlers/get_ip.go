package serverhandlers

import (
	"context"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

// GetIp handles GET /ips/{id}
func (a *Api) GetIp(c echo.Context, id string, params server.GetIpParams) error {
	return a.handleItem(c, "GetIp", "node_ip", "id", id, listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetIp(ctx, id, p)
	})
}
