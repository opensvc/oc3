package cdb

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

func logDuration(s string, begin time.Time) {
	slog.Debug(fmt.Sprintf("STAT: %s elapse: %s", s, time.Since(begin)))
}

func logDurationInfo(s string, begin time.Time) {
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
