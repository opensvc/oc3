package main

import (
	"context"
	"log/slog"

	"github.com/labstack/echo/v4"
	"github.com/shaj13/go-guardian/v2/auth/strategies/union"
	"github.com/spf13/viper"

	"github.com/opensvc/oc3/api"
	"github.com/opensvc/oc3/handlers"
	"github.com/opensvc/oc3/xauth"
)

func initListener() error {
	addr := viper.GetString("Listen")
	return listenAndServe(addr)
}

func listenAndServe(addr string) error {
	db, err := newDatabase()
	if err != nil {
		return err
	}
	e := echo.New()
	strategy := union.New(
		xauth.NewPublicStrategy("/public/"),
		xauth.NewBasicNode(db),
	)
	e.Use(handlers.AuthMiddleware(strategy))
	api.RegisterHandlers(e, &handlers.Api{
		DB:    db,
		Redis: newRedis(),
	})
	registerAPIUI(e)

	slog.Info("starting server on " + addr)
	return e.Start(addr)
}

func registerAPIUI(e *echo.Echo) {
	g := e.Group("/public/ui")
	g.Use(handlers.UIMiddleware(context.Background()))
}
