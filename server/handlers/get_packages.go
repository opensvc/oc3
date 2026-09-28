package serverhandlers

import (
	"context"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

// GetPackages handles GET /packages
func (a *Api) GetPackages(c echo.Context, params server.GetPackagesParams) error {
	odb := a.ODB
	return a.handleList(c, "GetPackages", "package", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
		filter: params.Filter,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return odb.GetPackages(ctx, p)
	})
}
