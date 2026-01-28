package cdb

import (
	"context"
	"errors"
	"strings"
)

type QFilterInput struct {
	BaseQuery string
	BaseArgs  []any

	SvcField   string
	NodeField  string
	GroupField string
	AppField   string
	UserField  string

	IsNode    bool
	IsSvc     bool
	IsManager bool

	NodeID   string
	NodeName string
	App      string
	SvcID    string

	UserGroups []string

	NodeSvcIDs        []string
	NodeApps          []string
	PublishedServices []string
	PublishedNodes    []string
	PublishedApps     []string
	UserGroupIDs      []int64

	ResolveNodeSvcIDs        func(ctx context.Context, nodeID, nodeName, app string) ([]string, error)
	ResolveNodeApps          func(ctx context.Context, nodeID string) ([]string, error)
	ResolvePublishedServices func(ctx context.Context) ([]string, error)
	ResolvePublishedNodes    func(ctx context.Context) ([]string, error)
	ResolvePublishedApps     func(ctx context.Context) ([]string, error)
	ResolveUserGroupIDs      func(ctx context.Context) ([]int64, error)
}

// NodeAccessFilter returns a SQL clause and args to restrict node access by team_responsible.
// If isManager is true, no filter is returned.
// If groups is empty, only nodes with team_responsible = 'Everybody' are allowed.
func NodeAccessFilter(groups []string, isManager bool) (string, []any) {
	if isManager {
		return "", nil
	}

	clean := cleanGroups(groups)
	if len(clean) == 0 {
		return " AND nodes.team_responsible = 'Everybody'", nil
	}

	clause := " AND (nodes.team_responsible = 'Everybody' OR nodes.team_responsible IN (?"
	args := []any{clean[0]}
	for i := 1; i < len(clean); i++ {
		clause += ", ?"
		args = append(args, clean[i])
	}
	clause += "))"

	return clause, args
}

// PublishedNodeIDsForGroups returns node ids accessible by the provided groups.
// It includes nodes with team_responsible = 'Everybody'.
func (oDb *DB) PublishedNodeIDsForGroups(ctx context.Context, groups []string) ([]string, error) {
	clean := cleanGroups(groups)

	query := "SELECT node_id FROM nodes WHERE team_responsible = 'Everybody'"
	args := []any{}
	if len(clean) > 0 {
		query += " OR team_responsible IN (" + Placeholders(len(clean)) + ")"
		for _, g := range clean {
			args = append(args, g)
		}
	}

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return ids, nil
}

// PublishedAppsForGroups returns app names accessible by the provided groups.
func (oDb *DB) PublishedAppsForGroups(ctx context.Context, groups []string) ([]string, error) {
	clean := cleanGroups(groups)

	if len(clean) == 0 {
		return []string{}, nil
	}

	query := "SELECT DISTINCT apps.app FROM apps " +
		"JOIN apps_responsibles ON apps.id = apps_responsibles.app_id " +
		"JOIN auth_group ON apps_responsibles.group_id = auth_group.id " +
		"WHERE auth_group.role IN (" + Placeholders(len(clean)) + ")"
	args := []any{}
	for _, g := range clean {
		args = append(args, g)
	}

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	apps := []string{}
	for rows.Next() {
		var app string
		if err := rows.Scan(&app); err != nil {
			return nil, err
		}
		apps = append(apps, app)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return apps, nil
}

// QFilter builds a SQL WHERE clause to append to a query
func QFilter(ctx context.Context, in QFilterInput) (string, []any, error) {
	var (
		q    string
		args []any
	)

	switch {
	case in.SvcField != "":
		switch {
		case in.IsSvc:
			if in.SvcID == "" {
				return "", nil, errors.New("qfilter: missing svc id")
			}
			q, args = eqClause(in.SvcField, in.SvcID)
		case in.IsNode:
			ids := in.NodeSvcIDs
			if len(ids) == 0 && in.ResolveNodeSvcIDs != nil {
				var err error
				ids, err = in.ResolveNodeSvcIDs(ctx, in.NodeID, in.NodeName, in.App)
				if err != nil {
					return "", nil, err
				}
			}
			q, args = inClause(in.SvcField, toAnySlice(ids))
		case !in.IsManager:
			ids := in.PublishedServices
			if len(ids) == 0 && in.ResolvePublishedServices != nil {
				var err error
				ids, err = in.ResolvePublishedServices(ctx)
				if err != nil {
					return "", nil, err
				}
			}
			q, args = inClause(in.SvcField, toAnySlice(ids))
		}

	case in.NodeField != "":
		switch {
		case in.IsNode:
			if in.NodeID == "" {
				return "", nil, errors.New("qfilter: missing node id")
			}
			q, args = eqClause(in.NodeField, in.NodeID)
		case !in.IsManager:
			ids := in.PublishedNodes
			if len(ids) == 0 && in.ResolvePublishedNodes != nil {
				var err error
				ids, err = in.ResolvePublishedNodes(ctx)
				if err != nil {
					return "", nil, err
				}
			}
			q, args = inClause(in.NodeField, toAnySlice(ids))
		}

	case in.AppField != "":
		switch {
		case in.IsNode:
			apps := in.NodeApps
			if len(apps) == 0 && in.ResolveNodeApps != nil {
				var err error
				apps, err = in.ResolveNodeApps(ctx, in.NodeID)
				if err != nil {
					return "", nil, err
				}
			}
			q, args = inClause(in.AppField, toAnySlice(apps))
		case !in.IsManager:
			apps := in.PublishedApps
			if len(apps) == 0 && in.ResolvePublishedApps != nil {
				var err error
				apps, err = in.ResolvePublishedApps(ctx)
				if err != nil {
					return "", nil, err
				}
			}
			q, args = inClause(in.AppField, toAnySlice(apps))
		}
	}

	if in.GroupField != "" {
		if !in.IsNode && !in.IsSvc && !in.IsManager {
			q, args = inClause(in.GroupField, toAnySlice(in.UserGroups))
		}
	}

	if in.UserField != "" {
		if !in.IsNode && !in.IsSvc && !in.IsManager {
			ids := in.UserGroupIDs
			if len(ids) == 0 && in.ResolveUserGroupIDs != nil {
				var err error
				ids, err = in.ResolveUserGroupIDs(ctx)
				if err != nil {
					return "", nil, err
				}
			}
			q, args = inClause(in.UserField, toAnyInt64Slice(ids))
		}
	}

	return joinBaseQuery(in.BaseQuery, q), append(in.BaseArgs, args...), nil
}

func cleanGroups(groups []string) []string {
	clean := make([]string, 0, len(groups))
	for _, g := range groups {
		g = strings.TrimSpace(g)
		if g == "" || g == "Manager" {
			continue
		}
		clean = append(clean, g)
	}
	return clean
}

func joinBaseQuery(base string, q string) string {
	switch {
	case base == "" && q == "":
		return "1=1"
	case base == "":
		return q
	case q == "":
		return base
	default:
		return "(" + base + ") AND (" + q + ")"
	}
}

func eqClause(field string, value any) (string, []any) {
	return field + " = ?", []any{value}
}

func inClause(field string, values []any) (string, []any) {
	if len(values) == 0 {
		return "1=0", nil
	}
	clause := field + " IN (?"
	for i := 1; i < len(values); i++ {
		clause += ", ?"
	}
	clause += ")"
	return clause, values
}

func toAnySlice(values []string) []any {
	if len(values) == 0 {
		return nil
	}
	args := make([]any, 0, len(values))
	for _, v := range values {
		args = append(args, v)
	}
	return args
}

func toAnyInt64Slice(values []int64) []any {
	if len(values) == 0 {
		return nil
	}
	args := make([]any, 0, len(values))
	for _, v := range values {
		args = append(args, v)
	}
	return args
}
