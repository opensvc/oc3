package serverhandlers

import (
	"context"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

// GetResources handles GET /resources
func (a *Api) GetResources(c echo.Context, params server.GetResourcesParams) error {
	return a.handleList(c, "GetResources", "resourceList", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
		filter: params.Filter,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetResources(ctx, p)
	})
}

// GetResource handles GET /resources/{resource_id}
func (a *Api) GetResource(c echo.Context, resourceId string, params server.GetResourceParams) error {
	return a.handleItem(c, "GetResource", "resourceList", "id", resourceId, listEndpointParams{props: params.Props},
		func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
			return a.ODB.GetResource(ctx, resourceId, p)
		})
}
