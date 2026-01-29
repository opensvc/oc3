package main

import (
	"context"
	"log/slog"

	"github.com/labstack/echo-contrib/echoprometheus"
	"github.com/labstack/echo-contrib/pprof"
	"github.com/labstack/echo/v4"
	"github.com/shaj13/go-guardian/v2/auth/strategies/union"
	"github.com/spf13/viper"

	"github.com/opensvc/oc3/feeder"
	"github.com/opensvc/oc3/feederhandlers"
	"github.com/opensvc/oc3/xauth"
)

func startFeeder() error {
	addr := viper.GetString("listener_feeder.addr")
	return listenAndServeFeeder(addr)
}

func listenAndServeFeeder(addr string) error {
	enableUI := viper.GetBool("listener_feeder.ui.enable")

	db, err := newDatabase()
	if err != nil {
		return err
	}

	redisClient := newRedis()

	e := echo.New()
	e.HideBanner = true
	e.HidePort = true

	if viper.GetBool("listener_feeder.pprof.enable") {
		slog.Info("add handler /oc3/feed/api/public/pprof")
		// TODO: move to authenticated path
		pprof.Register(e, "/oc3/feed/api/public/pprof")
	}

	strategy := union.New(
		xauth.NewPublicStrategy("/oc3/feed/api/public/", "/oc3/feed/api/docs", "/oc3/feed/api/version"),
		xauth.NewBasicNode(db),
	)
	if viper.GetBool("listener_feeder.metrics.enable") {
		slog.Info("add handler /oc3/feed/api/public/metrics")
		e.Use(echoprometheus.NewMiddleware("oc3_feeder"))
		e.GET("/oc3/feed/api/public/metrics", echoprometheus.NewHandler())
	}
	e.Use(feederhandlers.AuthMiddleware(strategy))
	slog.Info("register openapi handlers with base url: /oc3/feed/api")
	feeder.RegisterHandlersWithBaseURL(e, &feederhandlers.Feeder{
		DB:          db,
		Redis:       redisClient,
		UI:          enableUI,
		SyncTimeout: viper.GetDuration("listener_feeder.sync.timeout"),
	}, "/oc3/feed/api")
	if enableUI {
		registerFeederUI(e)
	}
	slog.Info("listen on " + addr)
	return e.Start(addr)
}

func registerFeederUI(e *echo.Echo) {
	slog.Info("add handler /oc3/feed/api/docs/")
	g := e.Group("/oc3/feed/api/docs")
	g.Use(feederhandlers.UIMiddleware(context.Background()))
}
