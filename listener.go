package main

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/api"
	"github.com/opensvc/oc3/handlers"
)

func listenAndServe(addr string) error {
	e := echo.New()
	api.RegisterHandlers(e, handlers.New())
	g := e.Group("/public/ui")
	if true {
		g.Use(handlers.UIMiddleware(context.Background()))
	}
	slog.Info("Starting server on " + addr)
	return http.ListenAndServe(addr, e)
}
