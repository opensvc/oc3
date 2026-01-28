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
	"github.com/opensvc/oc3/apihandlers"
	"github.com/opensvc/oc3/xauth"
)

func startApiCollector() error {
	addr := viper.GetString("listener_api.addr")
	return listenAndServeCollector(addr)
}

func listenAndServeCollector(addr string) error {
	enableUI := viper.GetBool("listener_api.ui.enable")

	db, err := newDatabase()
	if err != nil {
		return err
	}

	redisClient := newRedis()

	e := echo.New()
	e.HideBanner = true
	e.HidePort = true

	if viper.GetBool("listener_api.pprof.enable") {
		slog.Info("add handler /oc3/api/public/pprof")
		pprof.Register(e, "/oc3/api/public/pprof")
	}

	strategy := union.New(
		xauth.NewPublicStrategy("/oc3/api/public/", "/oc3/api/docs", "/oc3/api/version", "/oc3/api/openapi"),
		xauth.NewBasicNode(db),
	)
	if viper.GetBool("listener_api.metrics.enable") {
		slog.Info("add handler /oc3/api/public/metrics")
		e.Use(echoprometheus.NewMiddleware("oc3_api"))
		e.GET("/oc3/api/public/metrics", echoprometheus.NewHandler())
	}
	e.Use(apihandlers.AuthMiddleware(strategy))
	slog.Info("register openapi handlers with base url: /oc3/api")
	api.RegisterHandlersWithBaseURL(e, &apihandlers.Api{
		DB:          db,
		Redis:       redisClient,
		UI:          enableUI,
		SyncTimeout: viper.GetDuration("listener_api.sync.timeout"),
	}, "/oc3/api")
	if enableUI {
		registerApiCollectorUI(e)
	}
	slog.Info("listen on " + addr)
	return e.Start(addr)
}

func registerApiCollectorUI(e *echo.Echo) {
	slog.Info("add handler /oc3/api/docs/")
	g := e.Group("/oc3/api/docs")
	g.Use(apihandlers.UIMiddleware(context.Background()))
}
