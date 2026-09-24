package cdb

import (
	"strings"

	"github.com/opensvc/oc3/schema"
)

// ColumnFilter is one column filter of a list request, already resolved to SQL.
// Col names the filtered column, so that the query builder joins its table when
// the column is not selected; Expr is the condition with "?" placeholders.
type ColumnFilter struct {
	Col  *schema.Col
	Expr string
	Args []any
}

// ListParams bundles the standard query parameters shared by all list endpoints.
// Groups and IsManager encode the caller's access control context.
type ListParams struct {
	Groups    []string
	IsManager bool

	// UserID is the authenticated user's auth_user.id, set only by the endpoints
	// whose access control references the caller's identity rather than just its
	// groups. Nil when the caller is not a user (node credentials) or when the
	// endpoint does not need it.
	UserID *int64

	Limit       int
	Offset      int
	Props       []string
	SelectExprs []string
	TypeHints   map[string]string // Used by scanRowsToMaps to convert []byte driver values to the correct type
	OrderBy     []string
	GroupBy     []string

	// Filters are the column filters of the request, combined with AND.
	Filters []ColumnFilter

	// CountOnly asks for the query of a list without its sort: the rows are only
	// counted, see OrderByClause.
	CountOnly bool
}

// HasGroup reports whether the caller belongs to the named group. Use it for the
// privileges that are not covered by IsManager, which only tracks "Manager".
func (p ListParams) HasGroup(role string) bool {
	for _, g := range p.Groups {
		if g == role {
			return true
		}
	}
	return false
}

func (p ListParams) OrderByClause(defaultClause string) string {
	if p.CountOnly {
		// A count does not depend on the order of the rows: sorting them first
		// would only cost.
		return ""
	}
	if len(p.OrderBy) == 0 {
		return "ORDER BY " + defaultClause
	}
	return "ORDER BY " + strings.Join(p.OrderBy, ", ")
}

func (p ListParams) GroupByClause(defaultClause string) string {
	if len(p.GroupBy) > 0 {
		return "GROUP BY " + strings.Join(p.GroupBy, ", ")
	}
	if defaultClause != "" {
		return "GROUP BY " + defaultClause
	}
	return ""
}

// FilterConditions returns the column filters as conditions to AND into a
// hand-built WHERE clause, and their arguments. Queries built with the query
// builder use Query.WhereFilters instead, which also resolves the joins.
func (p ListParams) FilterConditions() ([]string, []any) {
	conds := make([]string, 0, len(p.Filters))
	var args []any
	for _, f := range p.Filters {
		conds = append(conds, f.Expr)
		args = append(args, f.Args...)
	}
	return conds, args
}
