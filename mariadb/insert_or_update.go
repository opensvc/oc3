package mariadb

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"
)

type (
	InsertOrUpdate struct {
		Table    string
		Keys     []string
		Mappings Mappings
		Data     any

		names        []string
		placeholders []string
		updates      []string
		values       []any
	}
)

func (t *InsertOrUpdate) load() error {
	switch v := t.Data.(type) {
	case map[string]any:
		return t.loadLines([]any{v})
	case []any:
		return t.loadLines(v)
	default:
		return fmt.Errorf("unsupported data format")
	}
}

func (t *InsertOrUpdate) loadLines(data []any) error {
	if len(data) == 0 {
		return nil
	}

	for _, mapping := range t.Mappings {
		t.names = append(t.names, mapping.To)
		if !slices.Contains(t.Keys, mapping.To) {
			t.updates = append(t.updates, fmt.Sprintf("%s = VALUES(%s)", mapping.To, mapping.To))
		}
	}

	for _, line := range data {
		var placeholders []string
		d, ok := line.(map[string]any)
		if !ok {
			return fmt.Errorf("unsupported data line format")
		}
		for _, mapping := range t.Mappings {
			if mapping.Raw != "" {
				placeholders = append(placeholders, mapping.Raw)
				continue
			}
			var value any
			var key string
			if mapping.From != "" {
				key = mapping.From
			} else {
				key = mapping.To
			}
			if v, ok := d[key]; !ok {
				return fmt.Errorf("key '%s' not found", key)
			} else {
				value = v
			}
			if mapping.Get != nil {
				if v, err := mapping.Get(value); err != nil {
					return err
				} else {
					value = v
				}
			}
			if mapping.Modify != nil {
				if placeholder, values, err := mapping.Modify(value); err != nil {
					return err
				} else {
					placeholders = append(placeholders, placeholder)
					t.values = append(t.values, values...)
				}
			} else {

				placeholders = append(placeholders, "?")
				t.values = append(t.values, value)
			}
		}
		t.placeholders = append(t.placeholders, fmt.Sprintf("(%s)", strings.Join(placeholders, ", ")))
	}
	return nil
}

func (t *InsertOrUpdate) QueryContext(ctx context.Context, db *sql.DB) (*sql.Rows, error) {
	if err := t.load(); err != nil {
		return nil, err
	}
	if len(t.values) == 0 {
		return nil, nil
	}
	sql := t.SQL()
	//slog.Debug(fmt.Sprint(sql, t.values))
	return db.QueryContext(ctx, sql, t.values...)
}

func (t *InsertOrUpdate) SQL() string {
	s := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s",
		t.Table,
		strings.Join(t.names, ", "),
		strings.Join(t.placeholders, ", "),
	)
	if len(t.updates) > 0 {
		s += fmt.Sprintf(
			" ON DUPLICATE KEY UPDATE %s",
			strings.Join(t.updates, ", "),
		)
	}
	return s
}
