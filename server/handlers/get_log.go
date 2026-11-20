package serverhandlers

import (
	"context"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

// GetLog handles GET /logs/{log_id}, an event the user may read, see GetLogs.
func (a *Api) GetLog(c echo.Context, logId string, params server.GetLogParams) error {
	if err := requireUserAuth(c); err != nil {
		return err
	}
	userEmail, _ := c.Get(XUserEmail).(string)
	return a.handleItem(c, "GetLog", "log_event", "id", logId, listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetLog(ctx, logId, p, userEmail)
	})
}
