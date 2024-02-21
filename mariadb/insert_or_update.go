package mariadb

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"
	"time"
)

type (
	Raw          string
	AccessorFunc func(v any) (any, error)

	InsertOrUpdate struct {
		Table    string
		Keys     []string
		Mappings Mappings
		Accessor AccessorFunc
		Data     any
		Timeout  time.Duration

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
		d, ok := data[0].(map[string]any)
		if !ok {
			return fmt.Errorf("unsupported data line format")
		}
		if _, ok := d[mapping.From]; ok {
			t.names = append(t.names, mapping.To)
			if !slices.Contains(t.Keys, mapping.To) {
				t.updates = append(t.updates, fmt.Sprintf("%s = VALUES(%s)", mapping.To, mapping.To))
			}
		}
	}

	for _, line := range data {
		var placeholders []string
		d, ok := line.(map[string]any)
		if !ok {
			return fmt.Errorf("unsupported data line format")
		}
		for _, mapping := range t.Mappings {
			if value, ok := d[mapping.From]; ok {
				if v, ok := value.(Raw); ok {
					placeholders = append(placeholders, string(v))
					continue
				}
				if t.Accessor != nil {
					if v, err := t.Accessor(value); err != nil {
						return fmt.Errorf("accessor: %s: %w", mapping.From, err)
					} else {
						value = v
					}
				}
				placeholders = append(placeholders, "?")
				t.values = append(t.values, value)
			}
		}
		t.placeholders = append(t.placeholders, fmt.Sprintf("(%s)", strings.Join(placeholders, ", ")))
	}
	return nil
}

func (t *InsertOrUpdate) Query(db *sql.DB) (*sql.Rows, error) {
	if err := t.load(); err != nil {
		return nil, err
	}
	if len(t.values) == 0 {
		return nil, nil
	}
	ctx := context.Background()
	if t.Timeout > 0 {
		ctx, _ = context.WithTimeout(ctx, t.Timeout)
	}
	return db.QueryContext(ctx, t.SQL(), t.values...)
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
