package cdb

import (
	"context"
	"fmt"
	"strings"
)

// GetNodeComplianceLogs returns the comp_log rows attached to a given node.
func (oDb *DB) GetNodeComplianceLogs(ctx context.Context, nodeID string, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getNodeComplianceLogs: no select expressions")
	}
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") +
		" FROM comp_log WHERE comp_log.node_id = ?"
	args := []any{nodeID}

	if !p.IsManager {
		clean := cleanGroups(p.Groups)
		if len(clean) == 0 {
			query += " AND 1=0"
		} else {
			placeholders := Placeholders(len(clean))
			query += ` AND comp_log.node_id IN (
				SELECT n.node_id FROM nodes n
				JOIN apps a ON n.app = a.app
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
	query += " " + p.OrderByClause("comp_log.run_date DESC, comp_log.id DESC")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getNodeComplianceLogs: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// GetServiceComplianceLogs returns the comp_log rows attached to a given service.
func (oDb *DB) GetServiceComplianceLogs(ctx context.Context, svcID string, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getServiceComplianceLogs: no select expressions")
	}
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") +
		" FROM comp_log WHERE comp_log.svc_id = ?"
	args := []any{svcID}

	if !p.IsManager {
		clean := cleanGroups(p.Groups)
		if len(clean) == 0 {
			query += " AND 1=0"
		} else {
			placeholders := Placeholders(len(clean))
			query += ` AND comp_log.svc_id IN (
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
	query += " " + p.OrderByClause("comp_log.run_date DESC, comp_log.id DESC")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getServiceComplianceLogs: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}
