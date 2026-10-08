package serverhandlers

import (
	"context"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

// GetServicesActions handles GET /services_actions: the actions the agents ran
// on the services the caller may see, as the historical Actions view.
func (a *Api) GetServicesActions(c echo.Context, params server.GetServicesActionsParams) error {
	return a.handleList(c, "GetServicesActions", "service_action", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
		filter: params.Filter,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetServicesActions(ctx, p)
	})
}

// GetServicesAction handles GET /services_actions/{action_id}
func (a *Api) GetServicesAction(c echo.Context, actionId string, params server.GetServicesActionParams) error {
	return a.handleItem(c, "GetServicesAction", "service_action", "action_id", actionId, listEndpointParams{props: params.Props},
		func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
			return a.ODB.GetServicesAction(ctx, actionId, p)
		})
}
