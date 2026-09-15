package serverhandlers

import (
	"context"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

// GetFiltersets handles GET /filtersets
func (a *Api) GetFiltersets(c echo.Context, params server.GetFiltersetsParams) error {
	return a.handleList(c, "GetFiltersets", "filterset", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetFiltersets(ctx, p)
	})
}
