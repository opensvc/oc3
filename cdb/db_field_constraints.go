package cdb

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/opensvc/oc3/schema"
)

// FieldConstraint is the list of the values allowed for an attribute of a kind of
// object, in display order, and how strictly the api holds to it: "suggest" or
// "strict", or the pattern the whole value must match, in "regex" mode. Only the
// constrained attributes have a row: one without is free text, the default.
type FieldConstraint struct {
	ObjectKind string
	Field      string
	Mode       string
	Values     []string
	Pattern    string
	Updated    time.Time
	UpdatedBy  string
}

// FieldConstraints returns the constraints of the attributes of a kind of object.
func (oDb *DB) FieldConstraints(ctx context.Context, objectKind string) ([]FieldConstraint, error) {
	rows, err := oDb.DB.QueryContext(ctx,
		`SELECT field, mode, values_json, pattern, updated, updated_by FROM field_constraints
		WHERE object_kind = ? ORDER BY field`, objectKind)
	if err != nil {
		return nil, fmt.Errorf("FieldConstraints: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []FieldConstraint
	for rows.Next() {
		c := FieldConstraint{ObjectKind: objectKind}
		var raw string
		var updated sql.NullTime
		if err := rows.Scan(&c.Field, &c.Mode, &raw, &c.Pattern, &updated, &c.UpdatedBy); err != nil {
			return nil, fmt.Errorf("FieldConstraints scan: %w", err)
		}
		if err := json.Unmarshal([]byte(raw), &c.Values); err != nil {
			return nil, fmt.Errorf("FieldConstraints %s.%s values: %w", objectKind, c.Field, err)
		}
		c.Updated = updated.Time
		out = append(out, c)
	}
	return out, rows.Err()
}

// SetFieldConstraint creates or replaces the constraint of an attribute. Meant to
// run in a transaction.
func (oDb *DB) SetFieldConstraint(ctx context.Context, c FieldConstraint) error {
	// Without the HTML escaping of json.Marshal: the list derives its text from
	// this JSON (fieldConstraintsFrom), where "&" must read "&", not "\u0026".
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(c.Values); err != nil {
		return fmt.Errorf("SetFieldConstraint: %w", err)
	}
	raw := bytes.TrimSpace(buf.Bytes())
	if _, err := oDb.ExecContext(ctx,
		`INSERT INTO field_constraints (object_kind, field, mode, values_json, pattern, updated, updated_by)
		VALUES (?, ?, ?, ?, ?, NOW(), ?)
		ON DUPLICATE KEY UPDATE mode = VALUES(mode), values_json = VALUES(values_json),
			pattern = VALUES(pattern), updated = VALUES(updated), updated_by = VALUES(updated_by)`,
		c.ObjectKind, c.Field, c.Mode, string(raw), c.Pattern, c.UpdatedBy); err != nil {
		return fmt.Errorf("SetFieldConstraint: %w", err)
	}
	oDb.SetChange("field_constraints")
	return nil
}

// DeleteFieldConstraint makes an attribute free text again, the default, which has
// no row. It reports whether the attribute had a constraint. Meant to run in a
// transaction.
func (oDb *DB) DeleteFieldConstraint(ctx context.Context, objectKind, field string) (bool, error) {
	n, err := oDb.ExecContextAndCountRowsAffected(ctx,
		"DELETE FROM field_constraints WHERE object_kind = ? AND field = ?", objectKind, field)
	if err != nil {
		return false, fmt.Errorf("DeleteFieldConstraint: %w", err)
	}
	if n > 0 {
		oDb.SetChange("field_constraints")
	}
	return n > 0, nil
}

var columnName = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)

// ObjectStringValues returns the values an object holds in the given columns of
// its table, an empty string for a NULL; none when the object is not found. The
// table, key and column names are the caller's, never the client's: they are
// written in the query.
func (oDb *DB) ObjectStringValues(ctx context.Context, table, key, id string, columns []string) (map[string]string, error) {
	out := make(map[string]string, len(columns))
	if len(columns) == 0 {
		return out, nil
	}
	for _, name := range append([]string{table, key}, columns...) {
		if !columnName.MatchString(name) {
			return nil, fmt.Errorf("ObjectStringValues: invalid name %q", name)
		}
	}
	exprs := make([]string, len(columns))
	for i, col := range columns {
		exprs[i] = "COALESCE(CAST(" + col + " AS CHAR), '')"
	}
	values := make([]string, len(columns))
	dest := make([]any, len(columns))
	for i := range values {
		dest[i] = &values[i]
	}
	err := oDb.DB.QueryRowContext(ctx,
		"SELECT "+strings.Join(exprs, ", ")+" FROM "+table+" WHERE "+key+" = ?", id).Scan(dest...)
	if err == sql.ErrNoRows {
		return out, nil
	}
	if err != nil {
		return nil, fmt.Errorf("ObjectStringValues: %w", err)
	}
	for i, col := range columns {
		out[col] = values[i]
	}
	return out, nil
}

// fieldConstraintsFrom is the FROM clause of the constraint list: a derived table
// named after field_constraints, where each constraint also carries the number of
// its values and what it allows as text, the pattern or the values separated by
// ", ", so that they can be filtered and sorted like the other columns. The text
// is cut out of the JSON, which SetFieldConstraint writes without spaces.
const fieldConstraintsFrom = ` FROM (
	SELECT object_kind, field, mode, values_json, pattern, updated, updated_by,
		JSON_LENGTH(values_json) AS value_count,
		IF(mode = 'regex', pattern,
			REPLACE(SUBSTRING(values_json, 3, CHAR_LENGTH(values_json) - 4), '","', ', ')) AS allowed
	FROM field_constraints
) field_constraints`

// FieldConstraintTableCol is a column of the derived table of the constraint list.
func FieldConstraintTableCol(name string) *schema.Col {
	return &schema.Col{T: tFieldConstraints, Name: name, Nullable: true}
}

var tFieldConstraints = &schema.Table{Name: "field_constraints"}

// GetFieldConstraintList returns the constraints of every kind of object, with the
// column filters, sort and pagination of the request.
func (oDb *DB) GetFieldConstraintList(ctx context.Context, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("GetFieldConstraintList: no select expressions")
	}
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") + fieldConstraintsFrom + " WHERE 1 = 1"
	filterConds, args := p.FilterConditions()
	for _, cond := range filterConds {
		query += " AND " + cond
	}
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("field_constraints.object_kind, field_constraints.field")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("GetFieldConstraintList: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}
