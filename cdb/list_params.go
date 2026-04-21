package cdb

import "strings"

// ListParams bundles the standard query parameters shared by all list endpoints.
// Groups and IsManager encode the caller's access control context.
type ListParams struct {
	Groups      []string
	IsManager   bool
	Limit       int
	Offset      int
	Props       []string
	SelectExprs []string
	TypeHints   map[string]string // Used by scanRowsToMaps to convert []byte driver values to the correct type
	OrderBy     []string
	GroupBy     []string
}

func (p ListParams) OrderByClause(defaultClause string) string {
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
