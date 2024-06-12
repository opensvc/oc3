package mariadb

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"reflect"
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

// ModifyFromRFC3339 returns placeholder from optional RFC3339 datetime
func ModifyFromRFC3339(a any) (placeholder string, values []any, err error) {
	switch v := a.(type) {
	case string:
		var t time.Time
		t, err = time.Parse(time.RFC3339Nano, v)
		placeholder = "?"
		values = append(values, t)
	case nil:
		placeholder = "?"
		values = append(values, v)
	default:
		err = fmt.Errorf("ModifyDatetime can't analyse %v", a)
	}
	return
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

// ModifierToString return is a modify function that prepares string.
// sep is used to join []string results to string.
func ModifierToString(sep string) func(a any) (placeholder string, values []any, err error) {
	return func(a any) (placeholder string, values []any, err error) {
		switch v := a.(type) {
		case string:
			placeholder = "?"
			values = append(values, v)
		case []string:
			placeholder = "?"
			values = append(values, strings.Join(v, sep))
		case []interface{}:
			placeholder = "?"
			var l []string
			for _, i := range v {
				l = append(l, i.(string))
			}
			values = append(values, strings.Join(l, sep))
		case []byte:
			placeholder = "?"
			values = append(values, string(v))
		default:
			err = fmt.Errorf("ModifierToString can't analyse type %s", reflect.TypeOf(a))
		}
		return
	}
}

func ModifyFromBase64ToString(a any) (placeholder string, values []any, err error) {
	switch v := a.(type) {
	case string:
		var b []byte
		b, err = base64.StdEncoding.DecodeString(v)
		if err != nil {
			err = fmt.Errorf("ModifyFromBase64ToString can't decode")
			return
		}
		placeholder = "?"
		values = append(values, string(b))
	default:
		err = fmt.Errorf("ModifyFromBase64ToString can't analyse type %s", reflect.TypeOf(a))
	}
	return
}
