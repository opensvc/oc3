package cdb

import (
	"context"
	"fmt"

	"strings"
	"time"
)

func (oDb *DB) GetNodeChecks(ctx context.Context, nodeID string, p ListParams) ([]map[string]any, error) {
	defer logDuration("getNodeChecks", time.Now())

	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getNodeChecks: no select expressions")
	}

	query := "SELECT " + strings.Join(p.SelectExprs, ", ") +
		" FROM checks_live LEFT JOIN services ON services.svc_id = checks_live.svc_id" +
		" WHERE checks_live.node_id = ?"
	args := []any{nodeID}

	if !p.IsManager {
		clean := cleanGroups(p.Groups)
		if len(clean) == 0 {
			query += " AND checks_live.node_id IN (SELECT n.node_id FROM nodes n WHERE n.team_responsible = 'Everybody')"
		} else {
			placeholders := Placeholders(len(clean))
			query += " AND checks_live.node_id IN (" +
				"SELECT n.node_id FROM nodes n " +
				"WHERE n.team_responsible = 'Everybody' " +
				"OR n.team_responsible IN (" + placeholders + ")" +
				")"
			for _, g := range clean {
				args = append(args, g)
			}
		}
	}

	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("checks_live.chk_type, checks_live.chk_instance")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getNodeChecks: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

func (oDb *DB) GetServiceChecks(ctx context.Context, svcID string, p ListParams) ([]map[string]any, error) {
	defer logDuration("getServiceChecks", time.Now())

	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getServiceChecks: no select expressions")
	}

	query := "SELECT " + strings.Join(p.SelectExprs, ", ") +
		" FROM checks_live LEFT JOIN services ON services.svc_id = checks_live.svc_id" +
		" WHERE checks_live.svc_id = ?"
	args := []any{svcID}

	if !p.IsManager {
		clean := cleanGroups(p.Groups)
		if len(clean) == 0 {
			query += " AND 1=0"
		} else {
			placeholders := Placeholders(len(clean))
			query += ` AND checks_live.svc_id IN (
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
	query += " " + p.OrderByClause("checks_live.chk_type, checks_live.chk_instance")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getServiceChecks: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}
