package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/opensvc/oc3/schema"
)

func buildArraysQuery(selectExprs []string) (string, []any, error) {
	q := From(schema.TStorArray).
		RawSelect(selectExprs...).
		Where(schema.StorArrayID, ">", 0)

	query, args, err := q.Build()
	if err != nil {
		return "", nil, fmt.Errorf("buildArraysQuery: %w", err)
	}
	return query, args, nil
}

func (oDb *DB) GetArrays(ctx context.Context, p ListParams) ([]map[string]any, error) {
	query, args, err := buildArraysQuery(p.SelectExprs)
	if err != nil {
		return nil, err
	}
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

// ArrayIDByIdent returns the id of the array named ident, or of id ident, as the
// historical lib_array_id resolved it, and whether one is.
func (oDb *DB) ArrayIDByIdent(ctx context.Context, ident string) (int64, bool, error) {
	var id int64
	err := oDb.DB.QueryRowContext(ctx,
		"SELECT id FROM stor_array WHERE array_name = ? OR CAST(id AS CHAR) = ? ORDER BY array_name = ? DESC LIMIT 1",
		ident, ident, ident).Scan(&id)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, false, nil
	case err != nil:
		return 0, false, fmt.Errorf("arrayIDByIdent: %w", err)
	}
	return id, true, nil
}

// GetArrayDiskgroups returns the disk groups of an array, as the historical
// /arrays/<id>/diskgroups listed them, the columns of the array reachable as
// stor_array.<column>.
func (oDb *DB) GetArrayDiskgroups(ctx context.Context, arrayID int64, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getArrayDiskgroups: no columns selected")
	}
	q := From(schema.TStorArrayDG).
		RawSelect(p.SelectExprs...).
		LeftJoinOn(schema.TStorArray, "stor_array.id = stor_array_dg.array_id").
		Where(schema.StorArrayDGArrayID, "=", arrayID).
		WhereFilters(p.Filters)
	query, args, err := q.Build()
	if err != nil {
		return nil, fmt.Errorf("getArrayDiskgroups: %w", err)
	}
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("stor_array_dg.dg_name")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getArrayDiskgroups: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}
