package main

import (
	"context"
	"log/slog"

	"github.com/labstack/echo-contrib/echoprometheus"
	"github.com/labstack/echo-contrib/pprof"
	"github.com/labstack/echo/v4"
	"github.com/shaj13/go-guardian/v2/auth/strategies/union"
	"github.com/spf13/viper"

	"github.com/opensvc/oc3/apifeeder"
	"github.com/opensvc/oc3/apifeederhandlers"
	"github.com/opensvc/oc3/xauth"
)

var (
	mwProm = echoprometheus.NewMiddleware("oc3_apifeeder")
)

func startApi() error {
	addr := viper.GetString("listener_feed.addr")
	return listenAndServe(addr)
}

func listenAndServe(addr string) error {
	enableUI := viper.GetBool("listener_feed.ui.enable")

	db, err := newDatabase()
	if err != nil {
		return err
	}

	redisClient := newRedis()

	e := echo.New()
	e.HideBanner = true
	e.HidePort = true

	if viper.GetBool("listener_feed.pprof.enable") {
		slog.Info("add handler /oc3/public/pprof")
		// TODO: move to authenticated path
		pprof.Register(e, "/oc3/public/pprof")
	}

	strategy := union.New(
		xauth.NewPublicStrategy("/oc3/public/", "/oc3/docs", "/oc3/version/"),
		xauth.NewBasicNode(db),
	)
	if viper.GetBool("listener_feed.metrics.enable") {
		slog.Info("add handler /oc3/public/metrics")
		e.Use(mwProm)
		e.GET("/oc3/public/metrics", echoprometheus.NewHandler())
	}
	e.Use(apifeederhandlers.AuthMiddleware(strategy))
	slog.Info("register openapi handlers with base url: /oc3")
	apifeeder.RegisterHandlersWithBaseURL(e, &apifeederhandlers.Api{
		DB:          db,
		Redis:       redisClient,
		UI:          enableUI,
		SyncTimeout: viper.GetDuration("listener_feed.sync.timeout"),
	}, "/oc3")
	if enableUI {
		registerAPIUI(e)
	}
	slog.Info("listen on " + addr)
	return e.Start(addr)
}

func registerAPIUI(e *echo.Echo) {
	slog.Info("add handler /oc3/docs/")
	g := e.Group("/oc3/docs")
	g.Use(apifeederhandlers.UIMiddleware(context.Background()))
}
