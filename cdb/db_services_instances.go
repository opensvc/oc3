package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/opensvc/oc3/schema"
)

func buildServicesInstancesQuery(groups []string, isManager bool, selectExprs []string) (string, []any, error) {
	q := From(schema.TSvcmon).
		Via(schema.TServices).
		RawSelect(selectExprs...)

	if !isManager {
		cleanGroups := cleanGroups(groups)
		if len(cleanGroups) == 0 {
			q = q.WhereRaw("1=0")
		} else {
			args := make([]any, len(cleanGroups))
			for i, g := range cleanGroups {
				args[i] = g
			}
			q = q.WhereRaw(
				"services.svc_app IN ("+
					"SELECT a.app FROM apps a"+
					" JOIN apps_responsibles ar ON ar.app_id = a.id"+
					" JOIN auth_group ag ON ag.id = ar.group_id"+
					" WHERE ag.role IN ("+Placeholders(len(cleanGroups))+")"+
					")",
				args...,
			)
		}
	} else {
		q = q.Where(schema.SvcmonID, ">", 0)
	}

	query, args, err := q.Build()
	if err != nil {
		return "", nil, fmt.Errorf("buildServicesInstancesQuery: %w", err)
	}
	return query, args, nil
}

func (oDb *DB) GetServicesInstances(ctx context.Context, p ListParams) ([]map[string]any, error) {
	query, args, err := buildServicesInstancesQuery(p.Groups, p.IsManager, p.SelectExprs)
	if err != nil {
		return nil, err
	}
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("svcmon.svc_id, svcmon.node_id")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getServicesInstances: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// GetServiceNodeInstance fetches the service instance on a specific node.
func (oDb *DB) GetServiceNodeInstance(ctx context.Context, svcID, nodeID string, p ListParams) ([]map[string]any, error) {
	query, args, err := buildServicesInstancesQuery(p.Groups, p.IsManager, p.SelectExprs)
	if err != nil {
		return nil, err
	}
	query += " AND svcmon.svc_id = ? AND svcmon.node_id = ?"
	args = append(args, svcID, nodeID)
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getServiceNodeInstance: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// GetNodeServices fetches all service instances on a specific node.
func (oDb *DB) GetNodeServices(ctx context.Context, nodeID string, p ListParams) ([]map[string]any, error) {
	query, args, err := buildServicesInstancesQuery(p.Groups, p.IsManager, p.SelectExprs)
	if err != nil {
		return nil, err
	}
	query += " AND svcmon.node_id = ?"
	args = append(args, nodeID)
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("svcmon.svc_id")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getNodeServices: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

func (oDb *DB) DeleteServiceInstanceCascade(ctx context.Context, svcID, nodeID string) (int64, error) {
	if svcID == "" || nodeID == "" {
		return 0, fmt.Errorf("DeleteServiceInstanceCascade: empty svc_id or node_id")
	}
	svcmonCount, err := oDb.execCountContext(ctx,
		"DELETE FROM svcmon WHERE svc_id = ? AND node_id = ?", svcID, nodeID)
	if err != nil {
		return 0, fmt.Errorf("DeleteServiceInstanceCascade svcmon: %w", err)
	}
	if svcmonCount == 0 {
		return 0, nil
	}
	oDb.SetChange("svcmon")
	cascade := []string{"dashboard", "resmon", "resinfo", "checks_live"}
	for _, t := range cascade {
		query := "DELETE FROM " + t + " WHERE svc_id = ? AND node_id = ?"
		if _, err := oDb.ExecContext(ctx, query, svcID, nodeID); err != nil {
			return svcmonCount, fmt.Errorf("DeleteServiceInstanceCascade %s: %w", t, err)
		}
		oDb.SetChange(t)
	}
	return svcmonCount, nil
}

// GetServicesInstance fetches all instances of a single service by svc_id (UUID) or svcname.
func (oDb *DB) GetServicesInstance(ctx context.Context, svcID string, p ListParams) ([]map[string]any, error) {
	query, args, err := buildServicesInstancesQuery(p.Groups, p.IsManager, p.SelectExprs)
	if err != nil {
		return nil, err
	}
	query += " AND (svcmon.svc_id = ? OR services.svcname = ?)"
	args = append(args, svcID, svcID)
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("svcmon.node_id")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getServicesInstance: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// ServiceInstanceByID returns the svc_id and node_id of the service instance
func (oDb *DB) ServiceInstanceByID(ctx context.Context, id int) (string, string, error) {
	const query = "SELECT svc_id, node_id FROM svcmon WHERE id = ? LIMIT 1"
	var svcID, nodeID sql.NullString
	err := oDb.DB.QueryRowContext(ctx, query, id).Scan(&svcID, &nodeID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", nil
	}
	if err != nil {
		return "", "", fmt.Errorf("ServiceInstanceByID: %w", err)
	}
	return svcID.String, nodeID.String, nil
}
