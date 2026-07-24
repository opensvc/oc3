package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

type ActionQueueRow struct {
	ID     int64
	Status string
	NodeID string
	SvcID  string
}

func (oDb *DB) GetActionByID(ctx context.Context, id int64) (ActionQueueRow, bool, error) {
	const query = "SELECT id, COALESCE(status, ''), COALESCE(node_id, ''), COALESCE(svc_id, '') FROM action_queue WHERE id = ?"
	var r ActionQueueRow
	err := oDb.DB.QueryRowContext(ctx, query, id).Scan(&r.ID, &r.Status, &r.NodeID, &r.SvcID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return ActionQueueRow{}, false, nil
	case err != nil:
		return ActionQueueRow{}, false, fmt.Errorf("getActionByID: %w", err)
	}
	return r, true, nil
}

func (oDb *DB) GetActionByIDMapped(ctx context.Context, id int64, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getActionByIDMapped: no columns selected")
	}
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") + " FROM action_queue WHERE id = ?"
	rows, err := oDb.DB.QueryContext(ctx, query, id)
	if err != nil {
		return nil, fmt.Errorf("getActionByIDMapped: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

func (oDb *DB) UpdateActionStatus(ctx context.Context, id int64, status string, resetDequeued bool) error {
	query := "UPDATE action_queue SET status = ? WHERE id = ?"
	if resetDequeued {
		query = "UPDATE action_queue SET status = ?, date_dequeued = 0 WHERE id = ?"
	}
	if _, err := oDb.DB.ExecContext(ctx, query, status, id); err != nil {
		return fmt.Errorf("updateActionStatus: %w", err)
	}
	oDb.SetChange("action_queue")
	return nil
}

func buildActionsQuery(p ListParams) (string, []any, error) {
	if len(p.SelectExprs) == 0 {
		return "", nil, fmt.Errorf("buildActionsQuery: no columns selected")
	}

	sb := &strings.Builder{}
	fmt.Fprintf(sb, "SELECT %s\nFROM action_queue", strings.Join(p.SelectExprs, ", "))

	var args []any
	if !p.IsManager {
		cleanGroups := cleanGroups(p.Groups)
		if len(cleanGroups) == 0 {
			sb.WriteString("\nWHERE 1=0")
		} else {
			fmt.Fprintf(sb,
				"\nWHERE node_id IN ("+
					"SELECT n.node_id FROM nodes n"+
					" JOIN apps a ON n.app = a.app"+
					" JOIN apps_responsibles ar ON ar.app_id = a.id"+
					" JOIN auth_group ag ON ag.id = ar.group_id"+
					" WHERE ag.role IN (%s)"+
					")",
				Placeholders(len(cleanGroups)),
			)
			args = append(args, stringsToAny(cleanGroups)...)
		}
	}

	return sb.String(), args, nil
}

// GetActions lists service and node actions posted in the action_queue.
func (oDb *DB) GetActions(ctx context.Context, p ListParams) ([]map[string]any, error) {
	query, args, err := buildActionsQuery(p)
	if err != nil {
		return nil, err
	}
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("action_queue.id")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getActions: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}
