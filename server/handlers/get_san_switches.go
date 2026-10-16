package serverhandlers

import (
	"context"
	"strconv"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

// GetSanSwitches handles GET /san-switches
func (a *Api) GetSanSwitches(c echo.Context, params server.GetSanSwitchesParams) error {
	odb := a.ODB
	return a.handleList(c, "GetSanSwitches", "switch", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
		filter: params.Filter,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return odb.GetSwitches(ctx, p)
	})
}

// GetSanSwitchPort handles GET /san-switches/{port_id}: one port, as a row of the
// list.
func (a *Api) GetSanSwitchPort(c echo.Context, portId int, params server.GetSanSwitchPortParams) error {
	odb := a.ODB
	return a.handleItem(c, "GetSanSwitchPort", "switch", "port_id", strconv.Itoa(portId), listEndpointParams{
		props: params.Props,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return odb.GetSwitchPort(ctx, portId, p)
	})
}
