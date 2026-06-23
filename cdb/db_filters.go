package cdb

import (
	"context"
	"fmt"
	"strings"
)

func (oDb *DB) GetFiltersetFilters(ctx context.Context, fsetID int, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getFiltersetFilters: no select expressions")
	}
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") +
		" FROM gen_filtersets_filters" +
		" JOIN gen_filters ON gen_filtersets_filters.f_id = gen_filters.id" +
		" WHERE gen_filtersets_filters.fset_id = ?"
	args := []any{fsetID}
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("gen_filtersets_filters.f_order, gen_filtersets_filters.id")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getFiltersetFilters: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// GetFilter returns a single gen_filters row by id.
func (oDb *DB) GetFilter(ctx context.Context, id string, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getFilter: no select expressions")
	}
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") +
		" FROM gen_filters WHERE gen_filters.id = ?"
	args := []any{id}
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getFilter: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// GetFilters returns rows from the gen_filters table.
func (oDb *DB) GetFilters(ctx context.Context, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getFilters: no select expressions")
	}
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") +
		" FROM gen_filters WHERE gen_filters.id > 0"
	args := []any{}
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("gen_filters.f_table, gen_filters.f_field, gen_filters.id")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getFilters: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}
