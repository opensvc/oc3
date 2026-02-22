package echolog

import (
	"context"
	"log/slog"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"

	"github.com/opensvc/oc3/util/logkey"
)

const (
	LoggerKey = "logger"
)

func GetLog(c echo.Context) *slog.Logger {
	if l, ok := c.Get(LoggerKey).(*slog.Logger); ok {
		return l
	}
	return slog.Default()
}

func SetLog(c echo.Context, l *slog.Logger) {
	c.Set(LoggerKey, l)
}

func GetLogHandler(c echo.Context, handler string) *slog.Logger {
	return GetLog(c).With(logkey.Handler, handler)
}

func LogRequestMiddleware(ctx context.Context, level slog.Level) echo.MiddlewareFunc {
	return middleware.RequestLoggerWithConfig(middleware.RequestLoggerConfig{
		LogStatus:   true,
		LogURIPath:  true,
		LogError:    true,
		LogRemoteIP: true,
		LogMethod:   true,
		HandleError: true, // forwards error to the global error handler, so it can decide appropriate status code
		LogValuesFunc: func(c echo.Context, v middleware.RequestLoggerValues) error {
			slog.LogAttrs(ctx, level, "request",
				slog.String(logkey.Method, v.Method),
				slog.String(logkey.URI, v.URIPath),
				slog.Int(logkey.StatusCode, v.Status),
				slog.String(logkey.RemoteIP, v.RemoteIP),
			)
			return nil
		},
	})
}
