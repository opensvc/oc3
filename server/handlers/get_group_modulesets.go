package serverhandlers

import (
	"context"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

// GetGroupModulesets handles GET /groups/{group_id}/modulesets
func (a *Api) GetGroupModulesets(c echo.Context, groupId string, params server.GetGroupModulesetsParams) error {
	return a.handleList(c, "GetGroupModulesets", "moduleset", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetGroupModulesets(ctx, groupId, p)
	})
}
