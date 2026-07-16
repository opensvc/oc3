package serverhandlers

import (
	"context"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

// GetGroupRulesets handles GET /groups/{group_id}/rulesets
func (a *Api) GetGroupRulesets(c echo.Context, groupId string, params server.GetGroupRulesetsParams) error {
	return a.handleList(c, "GetGroupRulesets", "ruleset", listEndpointParams{
		props: params.Props, limit: params.Limit, offset: params.Offset,
		meta: params.Meta, stats: params.Stats, orderby: params.Orderby, groupby: params.Groupby,
	}, func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
		return a.ODB.GetGroupRulesets(ctx, groupId, p)
	})
}
