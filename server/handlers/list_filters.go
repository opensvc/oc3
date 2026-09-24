package serverhandlers

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/server"
)

// likeEscaper protects the LIKE wildcards of a text filter: "50%" searches for the
// string "50%", not for anything starting with "50".
var likeEscaper = strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)

// buildFilters turns the filter query parameters of a list request into SQL
// conditions, combined with AND by the caller.
//
// Each value is "prop:expr". The prop is resolved like an orderby prop, joined
// props included, and must have a column. The expr is one of:
//
//	text        case-insensitive match anywhere in the value (LIKE %text%)
//	~regex      regular expression, case-insensitive (REGEXP)
//	in:a,b      one of the listed values
//	eq:v ne:v   equal, not equal
//	gt: gte: lt: lte:   comparisons
//	empty !empty        no value, any value
//
// Case-insensitivity comes from the utf8_general_ci collation of the collector
// tables, which LIKE and REGEXP both honour. Regular expressions are checked with
// Go's RE2 parser first: an invalid one is a 400 naming the column rather than a
// database error, and RE2 is a subset of the PCRE MariaDB evaluates them with.
func buildFilters(filters *server.InQueryFilter, mapping propMapping) ([]cdb.ColumnFilter, error) {
	if filters == nil {
		return nil, nil
	}
	out := make([]cdb.ColumnFilter, 0, len(*filters))
	for _, raw := range *filters {
		prop, expr, ok := strings.Cut(raw, ":")
		prop = strings.TrimSpace(prop)
		if !ok || prop == "" {
			return nil, fmt.Errorf("invalid filter %q: expected prop:expr", raw)
		}
		col, err := resolvePropCol(prop, mapping, "filter")
		if err != nil {
			return nil, err
		}
		if expr == "" {
			// An empty filter matches everything: nothing to add.
			continue
		}
		column := col.Qualified()
		f := cdb.ColumnFilter{Col: col}

		switch {
		case expr == "empty":
			f.Expr = fmt.Sprintf("(%s IS NULL OR %s = '')", column, column)
		case expr == "!empty":
			f.Expr = fmt.Sprintf("(%s IS NOT NULL AND %s <> '')", column, column)
		case strings.HasPrefix(expr, "~"):
			pattern := expr[1:]
			if _, err := regexp.Compile(pattern); err != nil {
				return nil, fmt.Errorf("invalid regular expression for %s: %v", prop, err)
			}
			f.Expr = column + " REGEXP ?"
			f.Args = []any{pattern}
		case strings.HasPrefix(expr, "in:"):
			values := strings.Split(expr[len("in:"):], ",")
			f.Expr = fmt.Sprintf("%s IN (%s)", column, cdb.Placeholders(len(values)))
			for _, v := range values {
				f.Args = append(f.Args, v)
			}
		default:
			op, value, isOp := comparison(expr)
			if isOp {
				f.Expr = column + " " + op + " ?"
				f.Args = []any{value}
			} else {
				f.Expr = column + " LIKE ?"
				f.Args = []any{"%" + likeEscaper.Replace(expr) + "%"}
			}
		}
		out = append(out, f)
	}
	return out, nil
}

// comparison recognises the "eq:", "ne:", "gt:"… operators of a filter.
func comparison(expr string) (op, value string, ok bool) {
	for prefix, sql := range map[string]string{
		"eq:": "=", "ne:": "<>", "gt:": ">", "gte:": ">=", "lt:": "<", "lte:": "<=",
	} {
		if strings.HasPrefix(expr, prefix) {
			return sql, expr[len(prefix):], true
		}
	}
	return "", "", false
}
