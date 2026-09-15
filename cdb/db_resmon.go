package cdb

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// GetServiceResourceLogs returns the resmon_log entries for a given service
func (oDb *DB) GetServiceResourceLogs(ctx context.Context, svcID string, p ListParams) ([]map[string]any, error) {
	defer logDuration("getServiceResourceLogs", time.Now())

	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getServiceResourceLogs: no select expressions")
	}

	query := "SELECT " + strings.Join(p.SelectExprs, ", ") +
		" FROM v_resmon_log" +
		" WHERE svc_id = ?"
	args := []any{svcID}

	if !p.IsManager {
		clean := cleanGroups(p.Groups)
		if len(clean) == 0 {
			query += " AND 1=0"
		} else {
			placeholders := Placeholders(len(clean))
			query += ` AND svc_id IN (
				SELECT s.svc_id FROM services s
				JOIN apps a ON s.svc_app = a.app
				JOIN apps_responsibles ar ON ar.app_id = a.id
				JOIN auth_group ag ON ag.id = ar.group_id
				WHERE ag.role IN (` + placeholders + `)
			)`
			for _, g := range clean {
				args = append(args, g)
			}
		}
	}

	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("res_begin DESC, node_id, rid")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getServiceResourceLogs: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// GetServiceNodeResourceLogs returns the v_resmon_log rows for a given service instance
func (oDb *DB) GetServiceNodeResourceLogs(ctx context.Context, svcID, nodeID string, p ListParams) ([]map[string]any, error) {
	defer logDuration("getServiceNodeResourceLogs", time.Now())

	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getServiceNodeResourceLogs: no select expressions")
	}

	query := "SELECT " + strings.Join(p.SelectExprs, ", ") +
		" FROM v_resmon_log" +
		" WHERE svc_id = ? AND node_id = ?"
	args := []any{svcID, nodeID}

	if !p.IsManager {
		clean := cleanGroups(p.Groups)
		if len(clean) == 0 {
			query += " AND 1=0"
		} else {
			placeholders := Placeholders(len(clean))
			query += ` AND svc_id IN (
				SELECT s.svc_id FROM services s
				JOIN apps a ON s.svc_app = a.app
				JOIN apps_responsibles ar ON ar.app_id = a.id
				JOIN auth_group ag ON ag.id = ar.group_id
				WHERE ag.role IN (` + placeholders + `)
			)`
			for _, g := range clean {
				args = append(args, g)
			}
		}
	}

	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("res_begin DESC, rid")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getServiceNodeResourceLogs: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// GetServiceResources returns the resmon rows for a given service
func (oDb *DB) GetServiceResources(ctx context.Context, svcID string, p ListParams) ([]map[string]any, error) {
	defer logDuration("getServiceResources", time.Now())

	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getServiceResources: no select expressions")
	}

	query := "SELECT " + strings.Join(p.SelectExprs, ", ") +
		" FROM resmon" +
		" WHERE resmon.svc_id = ?"
	args := []any{svcID}

	if !p.IsManager {
		clean := cleanGroups(p.Groups)
		if len(clean) == 0 {
			query += " AND 1=0"
		} else {
			placeholders := Placeholders(len(clean))
			query += ` AND resmon.svc_id IN (
				SELECT s.svc_id FROM services s
				JOIN apps a ON s.svc_app = a.app
				JOIN apps_responsibles ar ON ar.app_id = a.id
				JOIN auth_group ag ON ag.id = ar.group_id
				WHERE ag.role IN (` + placeholders + `)
			)`
			for _, g := range clean {
				args = append(args, g)
			}
		}
	}

	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("resmon.node_id, resmon.rid")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getServiceResources: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// GetServiceNodeResources returns the resmon rows for a given service instance
func (oDb *DB) GetServiceNodeResources(ctx context.Context, svcID, nodeID string, p ListParams) ([]map[string]any, error) {
	defer logDuration("getServiceNodeResources", time.Now())

	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getServiceNodeResources: no select expressions")
	}

	query := "SELECT " + strings.Join(p.SelectExprs, ", ") +
		" FROM resmon" +
		" WHERE resmon.svc_id = ? AND resmon.node_id = ?"
	args := []any{svcID, nodeID}

	if !p.IsManager {
		clean := cleanGroups(p.Groups)
		if len(clean) == 0 {
			query += " AND 1=0"
		} else {
			placeholders := Placeholders(len(clean))
			query += ` AND resmon.svc_id IN (
				SELECT s.svc_id FROM services s
				JOIN apps a ON s.svc_app = a.app
				JOIN apps_responsibles ar ON ar.app_id = a.id
				JOIN auth_group ag ON ag.id = ar.group_id
				WHERE ag.role IN (` + placeholders + `)
			)`
			for _, g := range clean {
				args = append(args, g)
			}
		}
	}

	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("resmon.rid")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getServiceNodeResources: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}
