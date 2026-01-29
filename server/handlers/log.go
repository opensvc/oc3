package serverhandlers

import (
	"log/slog"
)

func log(args ...any) {
	slog.Info("server", args...)
}

func logErr(args ...any) {
	slog.Error("server", args...)
}
