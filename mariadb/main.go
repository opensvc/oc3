package mariadb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

type (
	QueryContexter interface {
		QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	}

	Mappings []Mapping
	Mapping  struct {
		// To is the table column name
		To string

		// From is the loaded list element key name
		From string

		// Raw is the string to use as a placeholder
		Raw string

		// Get extracts the value from the initial data
		Get func(v any) (any, error)

		// Modify modifies the placeholder and value (ex: datetimes rfc change)
		Modify func(v any) (string, []any, error)

		// Optional may be set to true to ignore missing key during load data map
		// during InsertOrUpdate.QueryContext calls. It has no effect when loaded data is not
		// a map[string] any.
		Optional bool
	}
)

// ModifyDatetime returns placeholder for time.Time like objects
func ModifyDatetime(a any) (placeholder string, values []any, err error) {
	switch v := a.(type) {
	case string:
		// TODO: use default time.Parse instead to append time.Time value ?
		s := fmt.Sprint(v)
		if len(s) < 11 {
			// 2024-04-02
			placeholder = "?"
			values = append(values, s)
			return
		}
		if i := strings.LastIndex(s, "+"); i > 0 {
			placeholder = "CONVERT_TZ(?, ?, \"SYSTEM\")"
			values = append(values, s[:i], s[i:])
			return
		}
		if i := strings.LastIndex(s, "-"); i > 0 {
			placeholder = "CONVERT_TZ(?, ?, \"SYSTEM\")"
			values = append(values, s[:i], s[i:])
			return
		}
		placeholder = "?"
		values = append(values, s)
		return
	case time.Time:
		placeholder = "?"
		values = append(values, v)
		return
	default:
		err = fmt.Errorf("ModifyDatetime can't analyse %v", a)
		return
	}
}

func ModifierMaxLen(maxLen int) func(a any) (placeholder string, values []any, err error) {
	return func(a any) (placeholder string, values []any, err error) {
		switch v := a.(type) {
		case string:
			var value string
			if len(v) > maxLen {
				value = v[:maxLen]
			} else {
				value = v
			}
			placeholder = "?"
			values = append(values, value)
			return
		default:
			err = fmt.Errorf("ModifyStringLen can't analyse %v", a)
			return
		}
	}
}
