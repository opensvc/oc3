package mariadb

import (
	"fmt"
	"strings"
)

type (
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
	}
)

func ModifyDatetime(v any) (placeholder string, values []any, err error) {
	s := fmt.Sprint(v)
	if i := strings.Index(s, "+"); i > 0 {
		placeholder = "CONVERT_TZ(?, ?, \"SYSTEM\")"
		values = append(values, s[:i], s[i:])
		return
	}
	if i := strings.Index(s, "-"); i > 0 {
		placeholder = "CONVERT_TZ(?, ?, \"SYSTEM\")"
		values = append(values, s[:i], s[i:])
		return
	}
	placeholder = "?"
	values = append(values, s)
	return
}
