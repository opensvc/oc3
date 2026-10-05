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

// NotifyChanges publishes a "<table>_change" event for each table changed since
// the previous call, and forgets them: a session shared by many requests, as the
// api server's, would otherwise announce again every table changed since it
// started.
func (t *Session) NotifyChanges(ctx context.Context) error {
	slog.Debug("NotifyChanges")
	if t.ev == nil {
		return fmt.Errorf("NotifyChanges: eventPublisher is not configured")
	}
	for _, tableName := range t.takeChanges() {
		if err := t.NotifyTableChangeWithData(ctx, tableName, nil); err != nil {
			return err
		}
	}
	return nil
}

func (t *Session) NotifyTableChangeWithData(ctx context.Context, tableName string, data map[string]any) error {
	// Same guard as NotifyChanges: the server component creates its session with a
	// nil publisher, and dereferencing it here panicked the request after the write
	// had already been committed.
	if t.ev == nil {
		return fmt.Errorf("NotifyTableChangeWithData: eventPublisher is not configured")
	}
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

// takeChanges returns the changed tables and empties the list.
func (t *Session) takeChanges() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	r := make([]string, 0, len(t.tables))
	for s := range t.tables {
		r = append(r, s)
	}
	t.tables = make(map[string]struct{})
	return r
}
