package cdb

import (
	"fmt"
	"log/slog"
	"time"
)

func logDuration(s string, begin time.Time) {
	slog.Debug(fmt.Sprintf("STAT: %s elapse: %s", s, time.Now().Sub(begin)))
}

func logDurationInfo(s string, begin time.Time) {
	slog.Info(fmt.Sprintf("STAT: %s elapse: %s", s, time.Now().Sub(begin)))
}
