package serverhandlers

import (
	"context"
	"strconv"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

// GetNodesHardware handles GET /nodes/hardware
func (a *Api) GetNodesHardware(c echo.Context, params server.GetNodesHardwareParams) error {
	odb := a.ODB
	return a.handleList(c, "GetNodesHardware", "node_hw", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
		filter: params.Filter,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return odb.GetNodesHardware(ctx, p)
	})
}

// GetNodesHardwareComponent handles GET /nodes/hardware/{hw_id}: one component, as
// a row of the list, among those of the nodes the caller may see.
func (a *Api) GetNodesHardwareComponent(c echo.Context, hwId int, params server.GetNodesHardwareComponentParams) error {
	odb := a.ODB
	return a.handleItem(c, "GetNodesHardwareComponent", "node_hw", "hw_id", strconv.Itoa(hwId), listEndpointParams{
		props: params.Props,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return odb.GetNodeHardwareComponent(ctx, hwId, p)
	})
}
