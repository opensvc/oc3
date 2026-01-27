package apihandlers

import (
	"log/slog"
)

func log(args ...any) {
	slog.Info("apicollector", args...)
}

func logErr(args ...any) {
	slog.Error("apicollector", args...)
}
