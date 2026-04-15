package cdb

import (
	"context"
	"fmt"

	"github.com/opensvc/oc3/qb"
	"github.com/opensvc/oc3/schema"
)

func buildArraysQuery(selectExprs []string) (string, []any) {
	q := qb.From(schema.TStorArray).
		RawSelect(selectExprs...).
		Where(schema.StorArrayID, ">", 0)

	query, args, err := q.Build()
	if err != nil {
		panic(fmt.Sprintf("buildArraysQuery: %v", err))
	}
	return query, args
}

func (oDb *DB) GetArrays(ctx context.Context, p ListParams) ([]map[string]any, error) {
	query, args := buildArraysQuery(p.SelectExprs)
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("stor_array.array_name, stor_array.id")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getArrays: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}
