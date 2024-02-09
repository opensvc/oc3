package mariadb

import (
	"database/sql"
	"fmt"
	"slices"
	"strings"
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
	return db.Query(t.SQL(), t.values...)
}

func (t *InsertOrUpdate) SQL() string {
	return fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES %s ON DUPLICATE KEY UPDATE %s",
		t.Table,
		strings.Join(t.names, ", "),
		strings.Join(t.placeholders, ", "),
		strings.Join(t.updates, ", "),
	)
}
