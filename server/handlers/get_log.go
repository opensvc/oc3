package serverhandlers

import (
	"context"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

// GetLog handles GET /logs/{log_id}
func (a *Api) GetLog(c echo.Context, logId string, params server.GetLogParams) error {
	return a.handleItem(c, "GetLog", "log_event", "id", logId, listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetLog(ctx, logId, p)
	})
}
