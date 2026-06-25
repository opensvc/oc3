package cdb

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

type (
	LogEntry struct {
		ID          int64          `json:"id"`
		Action      string         `json:"log_action"`
		User        string         `json:"log_user"`
		Fmt         string         `json:"log_fmt"`
		Dict        map[string]any `json:"log_dict"`
		Date        time.Time      `json:"log_date"`
		SvcID       *uuid.UUID     `json:"svc_id"`
		IsGtalkSent bool           `json:"log_gtalk_sent"`
		IsEmailSent bool           `json:"log_email_sent"`
		EntryID     string         `json:"log_entry_id"`
		Level       string         `json:"log_level"`
		NodeID      *uuid.UUID     `json:"node_id"`
	}
)

type LogsFiltersetFilter struct {
	Active  bool
	NodeIDs []string
	SvcIDs  []string
}

// GetLogs returns rows from the collector log table.
func (oDb *DB) GetLogs(ctx context.Context, p ListParams, fset LogsFiltersetFilter) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getLogs: no select expressions")
	}
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") +
		" FROM log WHERE log.id > 0"
	args := []any{}
	if fset.Active {
		query, args = appendLogsFiltersetClause(query, args, fset.NodeIDs, fset.SvcIDs)
	}
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("log.id DESC")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getLogs: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

func appendLogsFiltersetClause(query string, args []any, nodeIDs, svcIDs []string) (string, []any) {
	nN := len(nodeIDs)
	nS := len(svcIDs)
	switch {
	case nN > 0 && nS > 0:
		query += " AND (log.node_id = '' OR log.node_id IN (" + Placeholders(nN) + "))"
		for _, id := range nodeIDs {
			args = append(args, id)
		}
		query += " AND (log.svc_id = '' OR log.svc_id IN (" + Placeholders(nS) + "))"
		for _, id := range svcIDs {
			args = append(args, id)
		}
	case nN == 1:
		query += " AND log.node_id = ?"
		args = append(args, nodeIDs[0])
	case nN > 0:
		query += " AND log.node_id IN (" + Placeholders(nN) + ")"
		for _, id := range nodeIDs {
			args = append(args, id)
		}
	case nS == 1:
		query += " AND log.svc_id = ?"
		args = append(args, svcIDs[0])
	case nS > 0:
		query += " AND log.svc_id IN (" + Placeholders(nS) + ")"
		for _, id := range svcIDs {
			args = append(args, id)
		}
	default:
		query += " AND log.node_id IS NULL"
	}
	return query, args
}

func (oDb *DB) Log(ctx context.Context, entries ...LogEntry) error {
	toDict := func(d map[string]any) string {
		if d == nil {
			return "{}"
		}
		s, err := json.Marshal(d)
		if err != nil {
			return "{}"
		}
		return string(s)
	}
	cols := "(log_action, log_user, log_fmt, log_dict, log_level, svc_id, node_id, log_date)"
	lines := make([]string, 0)
	args := make([]any, 0)

	for _, entry := range entries {
		args = append(args, entry.Action, entry.User, entry.Fmt, toDict(entry.Dict), entry.Level, entry.SvcID, entry.NodeID)
		lines = append(lines, "(?, ?, ?, ?, ?, ?, ?, NOW())")

	}
	sql := fmt.Sprintf("INSERT INTO log %s VALUES %s", cols, strings.Join(lines, ","))
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	_, err := oDb.ExecContext(ctx, sql, args...)
	return err
}
