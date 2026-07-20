package serverhandlers

import (
	"context"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// GetGroupHiddenMenuEntries handles GET /groups/{group_id}/hidden_menu_entries
func (a *Api) GetGroupHiddenMenuEntries(c echo.Context, groupId string, params server.GetGroupHiddenMenuEntriesParams) error {
	log := echolog.GetLogHandler(c, "GetGroupHiddenMenuEntries")
	ctx := c.Request().Context()

	group, found, err := a.ODB.AuthGroupByIDOrRole(ctx, groupId)
	if err != nil {
		log.Error("cannot resolve group", "group_id", groupId, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot resolve group %s", groupId)
	}
	if !found {
		return c.JSON(http.StatusOK, map[string]string{
			"info": fmt.Sprintf("Group %s does not exists", groupId),
		})
	}
	if group.Privilege {
		return JSONProblemf(c, http.StatusBadRequest, "Can not set hidden menu entries for privilege groups")
	}

	// Managers see every group; others only see the groups they belong to.
	allowed := IsManager(c) || HasGroup(c, group.Role)

	return a.handleList(c, "GetGroupHiddenMenuEntries", "hidden_menu_entry", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		if !allowed {
			return []map[string]any{}, nil
		}
		return a.ODB.GetGroupHiddenMenuEntries(ctx, group.ID, p)
	})
}
