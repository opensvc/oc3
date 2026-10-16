package serverhandlers

import (
	"context"
	"strconv"

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

// GetPackage handles GET /packages/{pkg_id}: one package, as a row of the list,
// among those of the nodes the caller may see.
func (a *Api) GetPackage(c echo.Context, pkgId int, params server.GetPackageParams) error {
	odb := a.ODB
	return a.handleItem(c, "GetPackage", "package", "pkg_id", strconv.Itoa(pkgId), listEndpointParams{
		props: params.Props,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return odb.GetPackage(ctx, pkgId, p)
	})
}
