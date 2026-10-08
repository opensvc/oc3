package serverhandlers

import (
	"context"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
)

// GetServiceActions handles GET /services/{svc_id}/actions: the actions the
// agents ran on the service, as the historical collector's service actions tab
// (table_actions_svc). A service unknown or out of the caller's reach is a 404,
// before any list.
func (a *Api) GetServiceActions(c echo.Context, svcId string, params server.GetServiceActionsParams) error {
	log := echolog.GetLogHandler(c, "GetServiceActions")
	svc, err := a.resolveServiceRow(c, log, c.Request().Context(), svcId)
	if err != nil {
		return err
	}
	if err := a.resolveService(c, log, svc.SvcID); err != nil {
		return err
	}
	return a.handleList(c, "GetServiceActions", "service_action", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
		filter: params.Filter,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetServiceActions(ctx, svc.SvcID, p)
	})
}
