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

// GetLogs returns rows from the collector log table.
func (oDb *DB) GetLogs(ctx context.Context, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getLogs: no select expressions")
	}
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") +
		" FROM log WHERE log.id > 0"
	args := []any{}
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
