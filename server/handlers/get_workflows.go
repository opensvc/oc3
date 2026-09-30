package serverhandlers

import (
	"context"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

// GetWorkflows handles GET /workflows, as the historical rest_get_workflows:
// every workflow is listed to any authenticated user. The assigned parameter
// narrows the list to the historical "Assigned to my team" and "Pending tiers
// action" tables.
func (a *Api) GetWorkflows(c echo.Context, params server.GetWorkflowsParams) error {
	odb := a.ODB
	assigned := ""
	if params.Assigned != nil {
		assigned = string(*params.Assigned)
	}
	switch assigned {
	case "", cdb.WorkflowsAssignedTeam, cdb.WorkflowsAssignedTiers:
	default:
		return JSONProblemf(c, http.StatusBadRequest, "invalid assigned value %q: expected %s or %s",
			assigned, cdb.WorkflowsAssignedTeam, cdb.WorkflowsAssignedTiers)
	}
	return a.handleList(c, "GetWorkflows", "workflow", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
		filter:     params.Filter,
		withUserID: assigned != "",
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return odb.GetWorkflows(ctx, nil, assigned, p)
	})
}
