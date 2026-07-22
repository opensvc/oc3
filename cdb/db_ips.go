package cdb

import (
	"context"
	"fmt"
	"strings"
)

func buildIpsQuery(p ListParams, idCond string, idArgs []any) (string, []any, error) {
	if len(p.SelectExprs) == 0 {
		return "", nil, fmt.Errorf("buildIpsQuery: no columns selected")
	}

	sb := &strings.Builder{}
	fmt.Fprintf(sb, "SELECT %s\nFROM v_nodenetworks", strings.Join(p.SelectExprs, ", "))

	var conds []string
	var args []any

	if idCond != "" {
		conds = append(conds, idCond)
		args = append(args, idArgs...)
	}

	if !p.IsManager {
		cleanGroups := cleanGroups(p.Groups)
		if len(cleanGroups) == 0 {
			conds = append(conds, "1=0")
		} else {
			conds = append(conds,
				"node_id IN ("+
					"SELECT n.node_id FROM nodes n"+
					" JOIN apps a ON n.app = a.app"+
					" JOIN apps_responsibles ar ON ar.app_id = a.id"+
					" JOIN auth_group ag ON ag.id = ar.group_id"+
					" WHERE ag.role IN ("+Placeholders(len(cleanGroups))+")"+
					")")
			args = append(args, stringsToAny(cleanGroups)...)
		}
	}

	if len(conds) > 0 {
		sb.WriteString("\nWHERE " + strings.Join(conds, " AND "))
	}

	return sb.String(), args, nil
}

func (oDb *DB) GetIps(ctx context.Context, p ListParams) ([]map[string]any, error) {
	query, args, err := buildIpsQuery(p, "", nil)
	if err != nil {
		return nil, err
	}
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("addr")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getIps: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

func (oDb *DB) GetIp(ctx context.Context, idOrAddr string, p ListParams) ([]map[string]any, error) {
	var idCond string
	if strings.ContainsAny(idOrAddr, ".:") {
		idCond = "addr = ?"
	} else {
		idCond = "id = ?"
	}
	query, args, err := buildIpsQuery(p, idCond, []any{idOrAddr})
	if err != nil {
		return nil, err
	}
	query += " " + p.OrderByClause("addr")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getIp: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}
