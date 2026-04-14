package serverhandlers

import (
	"fmt"
	"strings"

	"github.com/opensvc/oc3/server"
)

type ListQueryParameters struct {
	Page      PageParams
	Props     []string
	WithMeta  bool
	WithStats bool
	OrderBy   []string
	GroupBy   []string
}

func buildListQueryParameters(
	props *server.InQueryProps,
	limit *server.InQueryLimit,
	offset *server.InQueryOffset,
	meta *server.InQueryMeta,
	stats *server.InQueryStats,
	orderby *server.InQueryOrderby,
	groupby *server.InQueryGroupby,
	mapping propMapping,
) (ListQueryParameters, error) {
	selectedProps, err := buildProps(props, mapping)
	if err != nil {
		return ListQueryParameters{}, err
	}

	orderExprs, err := buildOrderBy(orderby, mapping)
	if err != nil {
		return ListQueryParameters{}, err
	}

	groupExprs, err := buildGroupBy(groupby, mapping)
	if err != nil {
		return ListQueryParameters{}, err
	}

	return ListQueryParameters{
		Page:      buildPageParams(limit, offset),
		Props:     selectedProps,
		WithMeta:  queryWithMeta(meta),
		WithStats: queryWithStats(stats),
		OrderBy:   orderExprs,
		GroupBy:   groupExprs,
	}, nil
}

func buildGroupBy(groupby *server.InQueryGroupby, mapping propMapping) ([]string, error) {
	if groupby == nil || *groupby == "" {
		return nil, nil
	}
	tokens := strings.Split(*groupby, ",")
	exprs := make([]string, 0, len(tokens))
	for _, token := range tokens {
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}
		def, ok := mapping.Props[token]
		if !ok {
			return nil, fmt.Errorf("unknown groupby prop %q", token)
		}
		col := def.Col
		if col == nil {
			return nil, fmt.Errorf("prop %q cannot be used in groupby (no column reference)", token)
		}
		exprs = append(exprs, col.Qualified())
	}
	return exprs, nil
}

func buildOrderBy(orderby *server.InQueryOrderby, mapping propMapping) ([]string, error) {
	if orderby == nil || *orderby == "" {
		return nil, nil
	}
	tokens := strings.Split(*orderby, ",")
	exprs := make([]string, 0, len(tokens))
	for _, token := range tokens {
		token = strings.TrimSpace(token)
		if token == "" {
			continue
		}
		desc := false
		if strings.HasPrefix(token, "-") {
			desc = true
			token = token[1:]
		}
		def, ok := mapping.Props[token]
		if !ok {
			return nil, fmt.Errorf("unknown orderby prop %q", token)
		}
		col := def.Col
		if col == nil {
			return nil, fmt.Errorf("prop %q cannot be used in orderby (no column reference)", token)
		}
		expr := col.Qualified()
		if desc {
			expr += " DESC"
		}
		exprs = append(exprs, expr)
	}
	return exprs, nil
}
