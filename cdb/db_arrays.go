package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/opensvc/oc3/schema"
)

func buildArraysQuery(selectExprs []string, filters []ColumnFilter) (string, []any, error) {
	q := From(schema.TStorArray).
		RawSelect(selectExprs...).
		Where(schema.StorArrayID, ">", 0).
		WhereFilters(filters)

	query, args, err := q.Build()
	if err != nil {
		return "", nil, fmt.Errorf("buildArraysQuery: %w", err)
	}
	return query, args, nil
}

func (oDb *DB) GetArrays(ctx context.Context, p ListParams) ([]map[string]any, error) {
	query, args, err := buildArraysQuery(p.SelectExprs, p.Filters)
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

// GetArrayDisks returns the luns of an array, as the historical
// /arrays/<id>/disks listed them: diskinfo joined to the uses the nodes report
// (svcdisks), one row per use, and to the array, the nodes and the services. A
// caller who is not a Manager sees the uses on the nodes of the apps their
// groups are responsible for, as the historical q_filter on svcdisks.node_id.
func (oDb *DB) GetArrayDisks(ctx context.Context, arrayID int64, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getArrayDisks: no columns selected")
	}
	q := From(schema.TDiskinfo).
		RawSelect(p.SelectExprs...).
		LeftJoinOn(schema.TSvcdisks, "svcdisks.disk_id = diskinfo.disk_id").
		LeftJoinOn(schema.TStorArray, "stor_array.array_name = diskinfo.disk_arrayid").
		LeftJoinOn(schema.TNodes, "nodes.node_id = svcdisks.node_id").
		LeftJoinOn(schema.TServices, "services.svc_id = svcdisks.svc_id").
		Where(schema.StorArrayID, "=", arrayID)
	if !p.IsManager {
		groups := cleanGroups(p.Groups)
		if len(groups) == 0 {
			q = q.WhereRaw("1=0")
		} else {
			q = q.WhereRaw("svcdisks.node_id IN ("+
				"SELECT n.node_id FROM nodes n"+
				" JOIN apps a ON n.app = a.app"+
				" JOIN apps_responsibles ar ON ar.app_id = a.id"+
				" JOIN auth_group ag ON ag.id = ar.group_id"+
				" WHERE ag.role IN ("+Placeholders(len(groups))+"))", stringsToAny(groups)...)
		}
	}
	q = q.WhereFilters(p.Filters)
	query, args, err := q.Build()
	if err != nil {
		return nil, fmt.Errorf("getArrayDisks: %w", err)
	}
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("diskinfo.disk_id, svcdisks.node_id")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getArrayDisks: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}
