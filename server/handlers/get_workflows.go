package serverhandlers

import (
	"context"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

// GetWorkflows handles GET /workflows, as the historical rest_get_workflows:
// every workflow is listed to any authenticated user.
func (a *Api) GetWorkflows(c echo.Context, params server.GetWorkflowsParams) error {
	odb := a.ODB
	return a.handleList(c, "GetWorkflows", "workflow", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
		filter: params.Filter,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return odb.GetWorkflows(ctx, nil, p)
	})
}
