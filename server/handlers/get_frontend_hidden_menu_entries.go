package serverhandlers

import (
	"context"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
)

// GetFrontendHiddenMenuEntries handles GET /frontend/hidden_menu_entries
func (a *Api) GetFrontendHiddenMenuEntries(c echo.Context, params server.GetFrontendHiddenMenuEntriesParams) error {
	log := echolog.GetLogHandler(c, "GetFrontendHiddenMenuEntries")

	// Managers see every group; others only see the groups they belong to.
	var groupIDs []int64
	if !IsManager(c) {
		ids, err := a.resolveUserGroupIDs(c, log)
		if err != nil {
			return err
		}
		// A non-nil (possibly empty) slice restricts the query to these groups.
		groupIDs = ids
		if groupIDs == nil {
			groupIDs = []int64{}
		}
	}

	return a.handleList(c, "GetFrontendHiddenMenuEntries", "hidden_menu_entry", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetAllHiddenMenuEntries(ctx, groupIDs, p)
	})
}
