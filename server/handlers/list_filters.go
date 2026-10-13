package serverhandlers

import (
	"fmt"
	"regexp"
	"strconv"
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
//	empty       no value
//	!expr       the inverse of any of the above: the rows expr leaves out
//
// A comparison value made of "-" and a duration in weeks, days, hours, minutes
// and seconds, "-15m" or "-1d12h", is a date relative to now, as in the
// historical collector: "lt:-15m" keeps the dates older than 15 minutes. The
// collector writes its dates with the database's NOW(), which the condition uses
// too.
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
		if def, ok := mapping.Props[prop]; ok && def.FilterExpr != "" {
			column = def.FilterExpr
		}
		cond, args, err := filterCondition(column, prop, expr)
		if err != nil {
			return nil, err
		}
		out = append(out, cdb.ColumnFilter{Col: col, Expr: cond, Args: args})
	}
	return out, nil
}

// filterCondition is the SQL condition of one filter expression on a column, with
// its arguments.
//
// A leading "!" inverts the rest of the expression. The inverse keeps the rows
// without a value: in SQL, NOT applied to a comparison with NULL is neither true
// nor false, and a bare NOT would drop from "not dev2n1" the rows that have no
// node at all. A "!" alone is not an inversion: it is the text "!".
func filterCondition(column, prop, expr string) (string, []any, error) {
	if inner, ok := strings.CutPrefix(expr, "!"); ok && inner != "" {
		if inner == "empty" {
			return fmt.Sprintf("(%s IS NOT NULL AND %s <> '')", column, column), nil, nil
		}
		cond, args, err := filterCondition(column, prop, inner)
		if err != nil {
			return "", nil, err
		}
		return fmt.Sprintf("(%s IS NULL OR NOT (%s))", column, cond), args, nil
	}

	switch {
	case expr == "empty":
		return fmt.Sprintf("(%s IS NULL OR %s = '')", column, column), nil, nil
	case strings.HasPrefix(expr, "~"):
		pattern := expr[1:]
		if _, err := regexp.Compile(pattern); err != nil {
			return "", nil, fmt.Errorf("invalid regular expression for %s: %v", prop, err)
		}
		return column + " REGEXP ?", []any{pattern}, nil
	case strings.HasPrefix(expr, "in:"):
		values := strings.Split(expr[len("in:"):], ",")
		args := make([]any, 0, len(values))
		for _, v := range values {
			args = append(args, v)
		}
		return fmt.Sprintf("%s IN (%s)", column, cdb.Placeholders(len(values))), args, nil
	}
	if op, value, isOp := comparison(expr); isOp {
		if seconds, relative := relativeSeconds(value); relative {
			return column + " " + op + " DATE_SUB(NOW(), INTERVAL ? SECOND)", []any{seconds}, nil
		}
		return column + " " + op + " ?", []any{value}, nil
	}
	return column + " LIKE ?", []any{"%" + likeEscaper.Replace(expr) + "%"}, nil
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

// relativeDate matches a date relative to now: "-" then at least one of weeks,
// days, hours, minutes and seconds, in that order.
var relativeDate = regexp.MustCompile(`^-(?:(\d+)w)?(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?$`)

// relativeSeconds returns the number of seconds a relative date such as "-1d12h"
// lies in the past, and whether value is one.
func relativeSeconds(value string) (int64, bool) {
	m := relativeDate.FindStringSubmatch(value)
	if m == nil || value == "-" {
		return 0, false
	}
	var seconds int64
	for i, unit := range []int64{7 * 86400, 86400, 3600, 60, 1} {
		if m[i+1] == "" {
			continue
		}
		n, err := strconv.ParseInt(m[i+1], 10, 64)
		if err != nil {
			return 0, false
		}
		seconds += n * unit
	}
	return seconds, true
}
