package serverhandlers

import (
	"context"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

// GetAlertEvent handles GET /alert_event
func (a *Api) GetAlertEvent(c echo.Context, params server.GetAlertEventParams) error {
	return a.handleList(c, "GetAlertEvent", "alert_event", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetAlertEvents(ctx, p)
	})
}
