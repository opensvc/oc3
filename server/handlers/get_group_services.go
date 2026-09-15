package serverhandlers

import (
	"context"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

// GetGroupServices handles GET /groups/{group_id}/services
func (a *Api) GetGroupServices(c echo.Context, groupId string, params server.GetGroupServicesParams) error {
	return a.handleList(c, "GetGroupServices", "service", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetGroupServices(ctx, groupId, p)
	})
}
