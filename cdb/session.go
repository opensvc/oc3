package cdb

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
)

type (
	Session struct {
		db     execContexter
		ev     eventPublisher
		tables map[string]struct{}
	}

	eventPublisher interface {
		EventPublish(eventName string, data map[string]any) error
	}
)

func NewSession(db execContexter, ev eventPublisher) *Session {
	return &Session{db: db, ev: ev, tables: make(map[string]struct{})}
}

func (t *Session) NotifyChanges(ctx context.Context) error {
	slog.Debug("NotifyChanges")
	if err := t.saveChanges(ctx); err != nil {
		return fmt.Errorf("NotifyChanges: %w", err)
	}
	if t.ev == nil {
		return fmt.Errorf("NotifyChanges: eventPublisher is not configured")
	}
	for _, tableName := range t.listChanges() {
		if err := t.NotifyTableChangeWithData(ctx, tableName, nil); err != nil {
			return err
		}
	}
	return nil
}

func (t *Session) NotifyTableChangeWithData(ctx context.Context, tableName string, data map[string]any) error {
	if err := t.ev.EventPublish(tableName+"_change", data); err != nil {
		return fmt.Errorf("EventPublish send %s: %w", tableName, err)
	}
	slog.Debug(fmt.Sprintf("table %s change notified", tableName))
	return nil
}

func (t *Session) SetChanges(s ...string) {
	for _, table := range s {
		t.tables[table] = struct{}{}
	}
}

func (t *Session) listChanges() []string {
	var r []string
	for s := range t.tables {
		r = append(r, s)
	}
	return r
}

func (t *Session) saveChanges(ctx context.Context) error {
	if len(t.tables) == 0 {
		return nil
	}
	var tables = make([]string, 0, len(t.tables))
	for table := range t.tables {
		tables = append(tables, fmt.Sprintf("('%s', NOW())", table))
	}
	query := fmt.Sprintf("INSERT INTO `table_modified` (`table_name`, `table_modified`) VALUES %s ON DUPLICATE KEY UPDATE `table_modified` = NOW()",
		strings.Join(tables, ","))
	_, err := t.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("saveChanges %s: %w", strings.Join(tables, ","), err)
	}
	return nil
}
