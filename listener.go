package main

import (
	"context"
	"log/slog"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/api"
	"github.com/opensvc/oc3/handlers"
)

func listenAndServe(addr string) error {
	e := echo.New()
	api.RegisterHandlers(e, handlers.New())
	registerXMLRPC(e)
	registerAPIUI(e)
	slog.Info("starting server on " + addr)
	return e.Start(addr)
}

func registerAPIUI(e *echo.Echo) {
	g := e.Group("/public/ui")
	g.Use(handlers.UIMiddleware(context.Background()))
}
