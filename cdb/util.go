package cdb

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"
)

func logDuration(s string, begin time.Time) {
	slog.Debug(fmt.Sprintf("STAT: %s elapse: %s", s, time.Since(begin)))
}

func logInfoDurationInfo(s string, begin time.Time) {
	slog.Info(fmt.Sprintf("STAT: %s elapse: %s", s, time.Since(begin)))
}

// helper function to check sql.Row scan result
func checkRow[T any](err error, valid bool, value T) (T, bool, error) {
	var zero T
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return zero, false, nil
	case err != nil:
		return zero, false, err
	case !valid:
		return zero, false, nil
	default:
		return value, true, nil
	}
}

// converts a raw value to the Go type indicated by kind.
func convertTyped(v any, kind string) any {
	b, ok := v.([]byte)
	if !ok {
		return v
	}
	s := string(b)
	switch kind {
	case "int64":
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return s
		}
		return n
	case "float64":
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return s
		}
		return f
	default:
		return s
	}
}

// Scans SQL rows into a slice of maps keyed by prop name.
func scanRowsToMaps(rows interface {
	Next() bool
	Scan(...any) error
	Err() error
}, props []string, typeHints ...map[string]string) ([]map[string]any, error) {
	var hints map[string]string
	if len(typeHints) > 0 {
		hints = typeHints[0]
	}
	results := make([]map[string]any, 0)
	for rows.Next() {
		ptrs := make([]any, len(props))
		vals := make([]any, len(props))
		for i := range ptrs {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("scanRowsToMaps: %w", err)
		}
		row := make(map[string]any, len(props))
		for i, prop := range props {
			row[prop] = convertTyped(vals[i], hints[prop])
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scanRowsToMaps rows: %w", err)
	}
	return results, nil
}

// Scans SQL rows into a slice of nested maps.
func scanRowsToNestedMaps(rows interface {
	Next() bool
	Scan(...any) error
	Err() error
}, props []string, primaryTable string) ([]map[string]any, error) {
	results := make([]map[string]any, 0)
	for rows.Next() {
		ptrs := make([]any, len(props))
		vals := make([]any, len(props))
		for i := range ptrs {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("scanRowsToNestedMaps: %w", err)
		}
		row := make(map[string]any)
		for i, prop := range props {
			var val any
			if b, ok := vals[i].([]byte); ok {
				val = string(b)
			} else {
				val = vals[i]
			}
			table, col, found := strings.Cut(prop, ".")
			if !found {
				table, col = primaryTable, prop
			}
			if row[table] == nil {
				row[table] = make(map[string]any)
			}
			row[table].(map[string]any)[col] = val
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scanRowsToNestedMaps rows: %w", err)
	}
	return results, nil
}

func appendLimitOffset(query string, args []any, limit, offset int) (string, []any) {
	if limit > 0 {
		query += " LIMIT ? OFFSET ?"
		args = append(args, limit, offset)
	}
	return query, args
}
