package cdb

import (
	"context"
	"fmt"
	"strings"
)

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
