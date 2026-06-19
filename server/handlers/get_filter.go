package serverhandlers

import (
	"context"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

// GetFilter handles GET /filters/{filter_id}
func (a *Api) GetFilter(c echo.Context, filterId string, params server.GetFilterParams) error {
	return a.handleItem(c, "GetFilter", "filter", "id", filterId, listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetFilter(ctx, filterId, p)
	})
}
