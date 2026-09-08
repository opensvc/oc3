package serverhandlers

import (
	"context"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

// GetObsolescenceSetting handles GET /obsolescence/settings/{id}
func (a *Api) GetObsolescenceSetting(c echo.Context, id string, params server.GetObsolescenceSettingParams) error {
	return a.handleItem(c, "GetObsolescenceSetting", "obsolescence", "id", id, listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetObsolescenceSetting(ctx, id, p)
	})
}
