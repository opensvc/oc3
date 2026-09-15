package serverhandlers

import (
	"context"
	"net/http"
	"slices"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// GetAlerts handles GET /alerts
func (a *Api) GetAlerts(c echo.Context, params server.GetAlertsParams) error {
	return a.handleAlerts(c, "GetAlerts", "", false,
		params.Props, params.Limit, params.Offset, params.Meta, params.Stats, params.Orderby, params.Groupby,
		func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
			return a.ODB.GetAlerts(ctx, p)
		})
}

// GetAlert handles GET /alerts/{id}
func (a *Api) GetAlert(c echo.Context, id string, params server.GetAlertParams) error {
	return a.handleAlerts(c, "GetAlert", id, true,
		params.Props, params.Limit, params.Offset, params.Meta, params.Stats, params.Orderby, params.Groupby,
		func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error) {
			return a.ODB.GetAlert(ctx, id, p)
		})
}

func (a *Api) handleAlerts(
	c echo.Context,
	handlerName string,
	itemVal string,
	isItem bool,
	props *server.InQueryProps,
	limit *server.InQueryLimit,
	offset *server.InQueryOffset,
	meta *server.InQueryMeta,
	stats *server.InQueryStats,
	orderby *server.InQueryOrderby,
	groupby *server.InQueryGroupby,
	fetch listFetcher,
) error {
	mapping := propsMapping["alert"]

	query, err := buildListQueryParameters(props, limit, offset, meta, stats, orderby, groupby, mapping)
	if err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	log := echolog.GetLogHandler(c, handlerName)

	hasAlert := slices.Contains(query.Props, "alert")
	fetchProps := query.Props
	if hasAlert {
		fetchProps = stripProp(fetchProps, "alert")
		fetchProps = ensureProps(fetchProps, "dash_fmt", "dash_dict")
	}

	selectExprs, err := buildSelectClause(fetchProps, mapping)
	if err != nil {
		log.Error("cannot build select clause", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot build select clause")
	}

	items, err := fetch(c.Request().Context(), cdb.ListParams{
		Groups:      UserGroupsFromContext(c),
		IsManager:   IsManager(c),
		Limit:       query.Page.Limit,
		Offset:      query.Page.Offset,
		Props:       fetchProps,
		SelectExprs: selectExprs,
		TypeHints:   buildTypeHints(fetchProps, mapping),
		OrderBy:     query.OrderBy,
		GroupBy:     query.GroupBy,
	})
	if err != nil {
		log.Error("cannot fetch alerts", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get alerts")
	}

	if hasAlert {
		mangleAlerts(items, query.Props)
	}

	if isItem && len(items) == 0 {
		return JSONProblemf(c, http.StatusNotFound, "alert %s not found", itemVal)
	}

	return c.JSON(http.StatusOK, newListResponse(items, mapping, query))
}
