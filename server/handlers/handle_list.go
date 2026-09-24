package serverhandlers

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
	"github.com/opensvc/oc3/util/echolog"
	"github.com/opensvc/oc3/util/logkey"
)

// listFetcher is the DB call signature shared by all list endpoints.
type listFetcher func(ctx context.Context, p cdb.ListParams) ([]map[string]any, error)

// listEndpointParams bundles the standard query parameters shared by every list endpoint.
type listEndpointParams struct {
	props   *server.InQueryProps
	limit   *server.InQueryLimit
	offset  *server.InQueryOffset
	meta    *server.InQueryMeta
	stats   *server.InQueryStats
	orderby *server.InQueryOrderby
	groupby *server.InQueryGroupby
	filter  *server.InQueryFilter

	// withUserID asks for the authenticated user's id to be forwarded in
	// cdb.ListParams. Set it only on the endpoints whose access control
	// references the caller's identity and not just its groups.
	withUserID bool
}

// handleList implements the common pipeline for all list endpoints:
//  1. Parse and validate query parameters (props, pagination, meta, stats)
//  2. Build SQL SELECT expressions from the resolved props
//  3. Build SQL JOIN fragments required by cross-table props
//  4. Call fetch to retrieve data from the database
//  5. Return a formatted JSON response with optional metadata
func (a *Api) handleList(
	c echo.Context,
	handlerName string,
	mappingKey string,
	p listEndpointParams,
	fetch listFetcher,
) error {
	mapping := propsMapping[mappingKey]

	query, err := buildListQueryParameters(p.props, p.limit, p.offset, p.meta, p.stats, p.orderby, p.groupby, mapping)
	if err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	filters, err := buildFilters(p.filter, mapping)
	if err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	log := echolog.GetLogHandler(c, handlerName)
	groups := UserGroupsFromContext(c)
	isManager := IsManager(c)

	log.Info("called",
		"limit", query.Page.Limit,
		"offset", query.Page.Offset,
		"props", query.Props,
		"meta", query.WithMeta,
		"stats", query.WithStats,
		"orderby", query.OrderBy,
		"groupby", query.GroupBy,
		"filters", len(filters),
		"is_manager", isManager,
	)

	selectExprs, err := buildSelectClause(query.Props, mapping)
	if err != nil {
		log.Error("cannot build select clause", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot build select clause")
	}

	dbParams := cdb.ListParams{
		Groups:      groups,
		IsManager:   isManager,
		Limit:       query.Page.Limit,
		Offset:      query.Page.Offset,
		Props:       query.Props,
		SelectExprs: selectExprs,
		TypeHints:   buildTypeHints(query.Props, mapping),
		OrderBy:     query.OrderBy,
		GroupBy:     query.GroupBy,
		Filters:     filters,
	}
	if p.withUserID {
		dbParams.UserID = authUserID(c)
	}

	items, err := fetch(c.Request().Context(), dbParams)
	if err != nil {
		log.Error("cannot fetch items", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get %s", mappingKey)
	}

	response := newListResponse(items, mapping, query)
	if query.WithMeta && !query.WithStats {
		response = response.withTotal(listTotal(c.Request().Context(), log, fetch, dbParams, len(items)))
	}
	return c.JSON(http.StatusOK, response)
}

// totalFromPage returns the list total when the page alone tells it: every row
// was asked for, the page is not full, or the first page is empty. Only a full
// page, or an empty page past the first, needs a count.
func totalFromPage(limit, offset, pageLen int) (int, bool) {
	switch {
	case limit <= 0:
		// No limit: the offset is not applied either, the page is the whole list.
		return pageLen, true
	case pageLen > 0 && pageLen < limit:
		return offset + pageLen, true
	case pageLen == 0 && offset == 0:
		return 0, true
	}
	return 0, false
}

// listTotal counts the rows of a list without pagination, for meta.total. The
// fetcher runs again with the same access control, filters and grouping, but
// selects a window count instead of the columns, without sort and for one row:
// COUNT(*) OVER () counts the rows of the final result, so a grouped list counts
// its groups, as the historical collector did. A failed count is logged and the
// total left out, rather than failing a list whose rows were read.
func listTotal(ctx context.Context, log *slog.Logger, fetch listFetcher, p cdb.ListParams, pageLen int) *int {
	if total, known := totalFromPage(p.Limit, p.Offset, pageLen); known {
		return &total
	}
	count := p
	count.CountOnly = true
	count.SelectExprs = []string{"COUNT(*) OVER ()"}
	count.Props = []string{"total"}
	count.TypeHints = map[string]string{"total": "int64"}
	count.Limit = 1
	count.Offset = 0
	rows, err := fetch(ctx, count)
	if err != nil {
		log.Error("cannot count the list rows", logkey.Error, err)
		return nil
	}
	if len(rows) == 0 {
		return intPtr(0)
	}
	switch n := rows[0]["total"].(type) {
	case int64:
		return intPtr(int(n))
	case int:
		return intPtr(n)
	}
	log.Error("unexpected list count", "value", rows[0]["total"])
	return nil
}

// handleItem is like handleList but expects exactly one result and returns 404
// when none is found. idKey and idVal are added to the "called" log entry.
func (a *Api) handleItem(
	c echo.Context,
	handlerName string,
	mappingKey string,
	idKey string,
	idVal string,
	p listEndpointParams,
	fetch listFetcher,
) error {
	mapping := propsMapping[mappingKey]

	query, err := buildListQueryParameters(p.props, p.limit, p.offset, p.meta, p.stats, p.orderby, p.groupby, mapping)
	if err != nil {
		return JSONProblem(c, http.StatusBadRequest, err.Error())
	}

	log := echolog.GetLogHandler(c, handlerName)
	groups := UserGroupsFromContext(c)
	isManager := IsManager(c)

	log.Info("called", idKey, idVal, "props", query.Props, "is_manager", isManager)

	selectExprs, err := buildSelectClause(query.Props, mapping)
	if err != nil {
		log.Error("cannot build select clause", logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot build select clause")
	}

	dbParams := cdb.ListParams{
		Groups:      groups,
		IsManager:   isManager,
		Limit:       query.Page.Limit,
		Offset:      query.Page.Offset,
		Props:       query.Props,
		SelectExprs: selectExprs,
	}
	if p.withUserID {
		dbParams.UserID = authUserID(c)
	}

	items, err := fetch(c.Request().Context(), dbParams)
	if err != nil {
		log.Error("cannot fetch item", idKey, idVal, logkey.Error, err)
		return JSONProblemf(c, http.StatusInternalServerError, "cannot get %s", mappingKey)
	}
	if len(items) == 0 {
		return JSONProblemf(c, http.StatusNotFound, "%s %s not found", mappingKey, idVal)
	}

	return c.JSON(http.StatusOK, newListResponse(items, mapping, query))
}
