package cdb

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

type (
	Session struct {
		db     execContexter
		ev     eventPublisher
		tables map[string]struct{}
		mu     sync.RWMutex
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
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, table := range s {
		t.tables[table] = struct{}{}
	}
}

func (t *Session) listChanges() []string {
	var r []string
	t.mu.RLock()
	defer t.mu.RUnlock()
	for s := range t.tables {
		r = append(r, s)
	}
	return r
}
