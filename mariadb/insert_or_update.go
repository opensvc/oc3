package mariadb

import (
	"database/sql"
	"fmt"
	"strings"
)

type (
	InsertOrUpdate struct {
		Keys         []string
		Columns      Columns
		names        []string
		placeholders []string
		updates      []string
		updateValues []any
		values       []any
	}
)

func (t Column) Names() []string {
	return append([]string{t.Name}, t.Alias...)
}

func (t *InsertOrUpdate) Add(name string, value any) {
	for _, k := range t.Keys {
		if k == name {
			t.addKey(name, value)
			return
		}
	}
	t.addNonKey(name, value)
}

func (t *InsertOrUpdate) AddString(name string, value string) {
	for _, k := range t.Keys {
		if k == name {
			t.addStringKey(name, value)
			return
		}
	}
	t.addStringNonKey(name, value)
}

func (t *InsertOrUpdate) Load(data map[string]any) {
	for _, column := range t.Columns {
		for _, name := range column.Names() {
			if value, ok := data[name]; ok {
				t.Add(name, value)
			}
		}
	}
}

func (t *InsertOrUpdate) Query(db *sql.DB) (*sql.Rows, error) {
	return db.Query(t.SQL(), append(t.values, t.updateValues...)...)
}

func (t *InsertOrUpdate) SQL() string {
	return fmt.Sprintf(
		"INSERT INTO nodes (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
		strings.Join(t.names, ", "),
		strings.Join(t.placeholders, ", "),
		strings.Join(t.updates, ", "),
	)
}

func (t *InsertOrUpdate) addStringNonKey(name string, value string) {
	t.names = append(t.names, name)
	t.placeholders = append(t.placeholders, value)
	t.updates = append(t.updates, fmt.Sprintf("%s = %s", name, value))
}

func (t *InsertOrUpdate) addStringKey(name string, value string) {
	t.names = append(t.names, name)
	t.placeholders = append(t.placeholders, value)
}

func (t *InsertOrUpdate) addNonKey(name string, value any) {
	t.names = append(t.names, name)
	t.updates = append(t.updates, fmt.Sprintf("%s = ?", name))
	t.placeholders = append(t.placeholders, "?")
	t.values = append(t.values, value)
	t.updateValues = append(t.updateValues, value)
}

func (t *InsertOrUpdate) addKey(name string, value any) {
	t.names = append(t.names, name)
	t.values = append(t.values, value)
	t.placeholders = append(t.placeholders, "?")
}
