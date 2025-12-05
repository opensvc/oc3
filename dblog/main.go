package dblog

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

type (
	Entry struct {
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

func Log(db *sql.DB, entries ...Entry) error {
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := db.ExecContext(ctx, sql, args...)
	return err
}
