package serverhandlers

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/util/logkey"
)

// statsTimeout bounds the statistics of a list: counting a large table under
// loose filters must answer "too long" rather than hold a connection.
const statsTimeout = 5 * time.Second

// errStatsTimeout is the statistics taking longer than statsTimeout.
var errStatsTimeout = errors.New("statistics took too long: narrow the selection with filters")

// listStatsResult holds the value counts of each prop of a stats request.
type listStatsResult struct {
	values   map[string]map[string]int
	distinct map[string]int
	// other counts, per prop, the rows whose value is not among those returned,
	// when the values are limited.
	other map[string]int
	total int
}

// listStats counts the distinct values of each prop over the whole selection of
// a list: access control, filters and session filterset of p apply, its
// pagination does not. With limit > 0, only the limit most frequent values of
// each prop are returned, the remainder being counted in other.
//
// The counts are made by the database, grouping the rows of the list on the
// value: the fetcher runs with the value and COUNT(*) as its select, grouped on
// the first one, sorted on the second. A prop the database cannot group on (a
// virtual prop, an aggregate expression) and the lists whose fetcher has a
// grouping of its own (inGo) are counted here instead, over every row.
func listStats(ctx context.Context, log *slog.Logger, fetch listFetcher, base cdb.ListParams, props []string, mapping propMapping, virtual map[string]virtualProp, limit int, inGo bool) (listStatsResult, error) {
	ctx, cancel := context.WithTimeout(ctx, statsTimeout)
	defer cancel()

	result := listStatsResult{
		values:   make(map[string]map[string]int, len(props)),
		distinct: make(map[string]int, len(props)),
		other:    make(map[string]int, len(props)),
	}
	base.Limit, base.Offset = 0, 0
	base.OrderBy, base.GroupBy = nil, nil
	base.CountOnly = false

	var inGoProps []string
	if inGo {
		inGoProps = props
	} else {
		total, err := statsTotal(ctx, fetch, base)
		if err != nil {
			return result, statsError(ctx, err)
		}
		result.total = total
		for _, prop := range props {
			if _, ok := virtual[prop]; ok {
				inGoProps = append(inGoProps, prop)
				continue
			}
			values, distinct, err := statsByDatabase(ctx, fetch, base, prop, mapping, limit)
			if err != nil {
				if ctx.Err() != nil {
					return result, errStatsTimeout
				}
				log.Warn("cannot count the values in the database, counting them here", "prop", prop, logkey.Error, err)
				inGoProps = append(inGoProps, prop)
				continue
			}
			result.values[prop] = values
			result.distinct[prop] = distinct
			result.other[prop] = total - sumCounts(values)
		}
	}
	if len(inGoProps) == 0 {
		return result, nil
	}

	fetchProps := fetchPropsFor(inGoProps, virtual)
	selectExprs, err := buildSelectClause(fetchProps, mapping)
	if err != nil {
		return result, err
	}
	p := base
	p.Props = fetchProps
	p.SelectExprs = selectExprs
	p.TypeHints = buildTypeHints(fetchProps, mapping)
	items, err := fetch(ctx, p)
	if err != nil {
		return result, statsError(ctx, err)
	}
	computeVirtualProps(items, inGoProps, virtual)
	result.total = len(items)
	values, distinct := buildStatsData(items, inGoProps)
	for _, prop := range inGoProps {
		top := topValues(values[prop], limit)
		result.values[prop] = top
		result.distinct[prop] = distinct[prop]
		result.other[prop] = len(items) - sumCounts(top)
	}
	return result, nil
}

// statsTotal counts the rows of the selection.
func statsTotal(ctx context.Context, fetch listFetcher, base cdb.ListParams) (int, error) {
	p := base
	p.CountOnly = true
	p.SelectExprs = []string{"COUNT(*)"}
	p.Props = []string{"total"}
	p.TypeHints = map[string]string{"total": "int64"}
	rows, err := fetch(ctx, p)
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}
	return countOf(rows[0]["total"])
}

// statsByDatabase returns the most frequent values of prop with their count, and
// the number of distinct values. NULL and blank values both count as "empty".
func statsByDatabase(ctx context.Context, fetch listFetcher, base cdb.ListParams, prop string, mapping propMapping, limit int) (map[string]int, int, error) {
	exprs, err := buildSelectClause([]string{prop}, mapping)
	if err != nil {
		return nil, 0, err
	}
	// By position: the select expression may carry an alias of its own.
	p := base
	p.SelectExprs = []string{exprs[0], "COUNT(*)"}
	p.Props = []string{"value", "count"}
	p.TypeHints = map[string]string{"count": "int64"}
	if kind := buildTypeHints([]string{prop}, mapping)[prop]; kind != "" {
		p.TypeHints["value"] = kind
	}
	p.GroupBy = []string{"1"}
	p.OrderBy = []string{"2 DESC", "1"}
	p.Limit = limit
	rows, err := fetch(ctx, p)
	if err != nil {
		return nil, 0, err
	}
	values := make(map[string]int, len(rows))
	for _, row := range rows {
		n, err := countOf(row["count"])
		if err != nil {
			return nil, 0, err
		}
		// "" and NULL make two groups for the database, one value here.
		values[statsValueKey(row["value"])] += n
	}
	if limit <= 0 || len(rows) < limit {
		return values, len(values), nil
	}

	// The values were cut: their number needs a count of the groups.
	p.CountOnly = true
	p.SelectExprs = []string{exprs[0], "COUNT(*) OVER ()"}
	p.Props = []string{"value", "groups"}
	p.TypeHints = map[string]string{"groups": "int64"}
	p.OrderBy = nil
	p.Limit = 1
	rows, err = fetch(ctx, p)
	if err != nil {
		return nil, 0, err
	}
	if len(rows) == 0 {
		return values, len(values), nil
	}
	groups, err := countOf(rows[0]["groups"])
	return values, groups, err
}

// statsLimit is the number of values a stats request asks for each prop: its
// limit when given, all of them otherwise.
func statsLimit(c echo.Context, query ListQueryParameters) int {
	if c.QueryParam("limit") == "" {
		return 0
	}
	return query.Page.Limit
}

// topValues keeps the limit most frequent values, the ties by value.
func topValues(values map[string]int, limit int) map[string]int {
	if limit <= 0 || len(values) <= limit {
		return values
	}
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b string) int {
		if values[a] != values[b] {
			return values[b] - values[a]
		}
		if a < b {
			return -1
		}
		return 1
	})
	top := make(map[string]int, limit)
	for _, k := range keys[:limit] {
		top[k] = values[k]
	}
	return top
}

func sumCounts(values map[string]int) int {
	n := 0
	for _, v := range values {
		n += v
	}
	return n
}

func countOf(v any) (int, error) {
	switch n := v.(type) {
	case int64:
		return int(n), nil
	case int:
		return n, nil
	}
	return 0, fmt.Errorf("unexpected count %v", v)
}

// statsError tells a timeout from another failure.
func statsError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return errStatsTimeout
	}
	return err
}
