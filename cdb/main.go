package cdb

import "strings"

func Placeholders(n int) string {
	return strings.TrimRight(strings.Repeat("?,", n), ",")
}
