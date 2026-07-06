package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
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

func (oDb *DB) FilterID(ctx context.Context, idOrLabel string) (int, bool, error) {
	if id, err := strconv.Atoi(idOrLabel); err == nil {
		return id, true, nil
	}
	var id int
	err := oDb.DB.QueryRowContext(ctx,
		"SELECT id FROM gen_filters WHERE f_label = ? LIMIT 1", idOrLabel).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("FilterID: %w", err)
	}
	return id, true, nil
}

type FilterRow struct {
	ID     int
	FTable string
	FField string
	FOp    string
	FValue string
	FLabel string
}

// GetFilterRow returns the gen_filters row for the given id, or nil when absent.
func (oDb *DB) GetFilterRow(ctx context.Context, id int) (*FilterRow, error) {
	const query = "SELECT id, f_table, f_field, f_op, f_value, f_label FROM gen_filters WHERE id = ? LIMIT 1"
	var (
		row            FilterRow
		fValue, fLabel sql.NullString
	)
	err := oDb.DB.QueryRowContext(ctx, query, id).Scan(&row.ID, &row.FTable, &row.FField, &row.FOp, &fValue, &fLabel)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("GetFilterRow: %w", err)
	}
	row.FValue = fValue.String
	row.FLabel = fLabel.String
	return &row, nil
}

type UpdateFilterFields struct {
	FTable *string
	FField *string
	FOp    *string
	FValue *string
	FLabel *string
}

func (oDb *DB) UpdateFilter(ctx context.Context, id int, fields UpdateFilterFields, author string) error {
	setClauses := []string{}
	args := []any{}
	if fields.FTable != nil {
		setClauses = append(setClauses, "f_table = ?")
		args = append(args, *fields.FTable)
	}
	if fields.FField != nil {
		setClauses = append(setClauses, "f_field = ?")
		args = append(args, *fields.FField)
	}
	if fields.FOp != nil {
		setClauses = append(setClauses, "f_op = ?")
		args = append(args, *fields.FOp)
	}
	if fields.FValue != nil {
		setClauses = append(setClauses, "f_value = ?")
		args = append(args, sql.NullString{String: *fields.FValue, Valid: true})
	}
	if fields.FLabel != nil {
		setClauses = append(setClauses, "f_label = ?")
		args = append(args, sql.NullString{String: *fields.FLabel, Valid: true})
	}
	setClauses = append(setClauses, "f_updated = NOW()", "f_author = ?")
	args = append(args, author)

	query := "UPDATE gen_filters SET " + strings.Join(setClauses, ", ") + " WHERE id = ?"
	args = append(args, id)
	if _, err := oDb.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("UpdateFilter: %w", err)
	}
	oDb.SetChange("gen_filters")
	return nil
}

// DeleteFilterCascade deletes a filter and its attachments to filtersets.
func (oDb *DB) DeleteFilterCascade(ctx context.Context, id int) error {
	if _, err := oDb.ExecContext(ctx,
		"DELETE FROM gen_filtersets_filters WHERE f_id = ?", id); err != nil {
		return fmt.Errorf("DeleteFilterCascade gen_filtersets_filters: %w", err)
	}
	oDb.SetChange("gen_filtersets_filters")

	if _, err := oDb.ExecContext(ctx, "DELETE FROM gen_filters WHERE id = ?", id); err != nil {
		return fmt.Errorf("DeleteFilterCascade gen_filters: %w", err)
	}
	oDb.SetChange("gen_filters")
	return nil
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
