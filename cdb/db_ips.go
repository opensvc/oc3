package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

type NodeIP struct {
	ID     int64
	Addr   string
	NodeID string
}

func (oDb *DB) GetNodeIPByID(ctx context.Context, id int64) (NodeIP, bool, error) {
	const query = "SELECT id, COALESCE(addr, ''), COALESCE(node_id, '') FROM node_ip WHERE id = ?"
	var ip NodeIP
	err := oDb.DB.QueryRowContext(ctx, query, id).Scan(&ip.ID, &ip.Addr, &ip.NodeID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return NodeIP{}, false, nil
	case err != nil:
		return NodeIP{}, false, fmt.Errorf("getNodeIPByID: %w", err)
	}
	return ip, true, nil
}

func (oDb *DB) DeleteNodeIP(ctx context.Context, id int64) (int64, error) {
	const query = "DELETE FROM node_ip WHERE id = ?"
	res, err := oDb.DB.ExecContext(ctx, query, id)
	if err != nil {
		return 0, fmt.Errorf("deleteNodeIP: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("deleteNodeIP rowsAffected: %w", err)
	}
	if n > 0 {
		oDb.SetChange("node_ip")
	}
	return n, nil
}

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
