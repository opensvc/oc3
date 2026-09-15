package serverhandlers

import (
	"context"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

// GetGroupApps handles GET /groups/{group_id}/apps
func (a *Api) GetGroupApps(c echo.Context, groupId string, params server.GetGroupAppsParams) error {
	return a.handleList(c, "GetGroupApps", "app", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetGroupApps(ctx, groupId, p)
	})
}
