package apifeederhandlers

import (
	"log/slog"

	"github.com/labstack/echo/v4"
)

var (
	defaultLogger = slog.Default()
)

func getLog(c echo.Context) *slog.Logger {
	if l, ok := c.Get("logger").(*slog.Logger); ok {
		return l
	}
	return defaultLogger
}
