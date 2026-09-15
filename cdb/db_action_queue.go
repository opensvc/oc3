package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

type ActionQueueRow struct {
	ID      int64
	Status  string
	Command string
	NodeID  string
	SvcID   string
}

func (oDb *DB) GetActionByID(ctx context.Context, id int64) (ActionQueueRow, bool, error) {
	const query = "SELECT id, COALESCE(status, ''), COALESCE(command, ''), COALESCE(node_id, ''), COALESCE(svc_id, '') FROM action_queue WHERE id = ?"
	var r ActionQueueRow
	err := oDb.DB.QueryRowContext(ctx, query, id).Scan(&r.ID, &r.Status, &r.Command, &r.NodeID, &r.SvcID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return ActionQueueRow{}, false, nil
	case err != nil:
		return ActionQueueRow{}, false, fmt.Errorf("getActionByID: %w", err)
	}
	return r, true, nil
}

func (oDb *DB) DeleteAction(ctx context.Context, id int64) (int64, error) {
	const query = "DELETE FROM action_queue WHERE id = ?"
	res, err := oDb.DB.ExecContext(ctx, query, id)
	if err != nil {
		return 0, fmt.Errorf("deleteAction: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("deleteAction rowsAffected: %w", err)
	}
	if n > 0 {
		oDb.SetChange("action_queue")
	}
	return n, nil
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

func buildActionsQuery(p ListParams, idCond string, idArgs []any) (string, []any, error) {
	if len(p.SelectExprs) == 0 {
		return "", nil, fmt.Errorf("buildActionsQuery: no columns selected")
	}

	sb := &strings.Builder{}
	fmt.Fprintf(sb, "SELECT %s\nFROM action_queue", strings.Join(p.SelectExprs, ", "))

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

// GetActions lists service and node actions posted in the action_queue.
func (oDb *DB) GetActions(ctx context.Context, p ListParams) ([]map[string]any, error) {
	query, args, err := buildActionsQuery(p, "", nil)
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

func (oDb *DB) GetActionOne(ctx context.Context, id string, p ListParams) ([]map[string]any, error) {
	query, args, err := buildActionsQuery(p, "action_queue.id = ?", []any{id})
	if err != nil {
		return nil, err
	}
	query += " " + p.OrderByClause("action_queue.id")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getActionOne: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}
