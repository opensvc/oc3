package serverhandlers

import (
	"context"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

// GetObsolescenceSettings handles GET /obsolescence/settings
func (a *Api) GetObsolescenceSettings(c echo.Context, params server.GetObsolescenceSettingsParams) error {
	return a.handleList(c, "GetObsolescenceSettings", "obsolescence", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetObsolescenceSettings(ctx, p)
	})
}
