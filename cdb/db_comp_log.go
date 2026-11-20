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

	// The runs of a node of an app published to the caller, as GET
	// /compliance/logs.
	cond, condArgs := compPublishedNodesCond("comp_log.node_id", p.Groups, p.IsManager)
	query += " AND " + cond
	args = append(args, condArgs...)

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

	// The runs on a node of an app published to the caller, as GET
	// /compliance/logs.
	cond, condArgs := compPublishedNodesCond("comp_log.node_id", p.Groups, p.IsManager)
	query += " AND " + cond
	args = append(args, condArgs...)

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
