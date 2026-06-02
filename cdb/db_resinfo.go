package cdb

import (
	"context"
	"fmt"
	"strings"
	"time"
)

func (oDb *DB) GetServiceResinfo(ctx context.Context, svcID string, p ListParams) ([]map[string]any, error) {
	defer logDuration("getServiceResinfo", time.Now())

	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getServiceResinfo: no select expressions")
	}

	query := "SELECT " + strings.Join(p.SelectExprs, ", ") +
		" FROM resinfo" +
		" WHERE resinfo.svc_id = ?"
	args := []any{svcID}

	if !p.IsManager {
		clean := cleanGroups(p.Groups)
		if len(clean) == 0 {
			query += " AND 1=0"
		} else {
			placeholders := Placeholders(len(clean))
			query += ` AND resinfo.svc_id IN (
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
	query += " " + p.OrderByClause("resinfo.rid, resinfo.res_key")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getServiceResinfo: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}
