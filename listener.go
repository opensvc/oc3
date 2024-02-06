package main

import (
	"context"
	"log/slog"

	"github.com/labstack/echo/v4"
	"github.com/shaj13/go-guardian/v2/auth/strategies/union"

	"github.com/opensvc/oc3/api"
	"github.com/opensvc/oc3/auth"
	"github.com/opensvc/oc3/handlers"
)

func listenAndServe(addr string) error {
	e := echo.New()
	strategy := union.New(
		auth.NewPublicStrategy("/public/"),
		auth.NewBasicNode(),
	)
	e.Use(handlers.AuthMiddleware(strategy))
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
