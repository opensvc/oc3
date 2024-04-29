package main

import (
	"context"
	"log/slog"

	"github.com/labstack/echo-contrib/echoprometheus"
	"github.com/labstack/echo-contrib/pprof"
	"github.com/labstack/echo/v4"
	"github.com/shaj13/go-guardian/v2/auth/strategies/union"
	"github.com/spf13/viper"

	"github.com/opensvc/oc3/api"
	"github.com/opensvc/oc3/handlers"
	"github.com/opensvc/oc3/xauth"
)

var (
	mwProm = echoprometheus.NewMiddleware("oc3_api")
)

func listen() error {
	addr := viper.GetString("listener.addr")
	return listenAndServe(addr)
}

func listenAndServe(addr string) error {
	enableUI := viper.GetBool("listener.ui.enable")

	db, err := newDatabase()
	if err != nil {
		return err
	}

	redisClient := newRedis()

	e := echo.New()
	e.HideBanner = true
	e.HidePort = true

	if viper.GetBool("listener.pprof.enable") {
		slog.Info("add handler /public/pprof")
		// TODO: move to authenticated path
		pprof.Register(e, "/public/pprof")
	}

	strategy := union.New(
		xauth.NewPublicStrategy("/public/"),
		xauth.NewBasicNode(db),
	)
	if viper.GetBool("listener.metrics.enable") {
		slog.Info("add handler /public/metrics")
		e.Use(mwProm)
		e.GET("/public/metrics", echoprometheus.NewHandler())
	}
	e.Use(handlers.AuthMiddleware(strategy))
	api.RegisterHandlers(e, &handlers.Api{
		DB:    db,
		Redis: redisClient,
		UI:    enableUI,
	})
	if enableUI {
		registerAPIUI(e)
	}
	slog.Info("listen on " + addr)
	return e.Start(addr)
}

func registerAPIUI(e *echo.Echo) {
	slog.Info("add handler /public/ui")
	g := e.Group("/public/ui")
	g.Use(handlers.UIMiddleware(context.Background()))
}
