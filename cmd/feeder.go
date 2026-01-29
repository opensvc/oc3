package cmd

import (
	"context"
	"log/slog"

	"github.com/labstack/echo-contrib/echoprometheus"
	"github.com/labstack/echo-contrib/pprof"
	"github.com/labstack/echo/v4"
	"github.com/shaj13/go-guardian/v2/auth/strategies/union"
	"github.com/spf13/viper"

	"github.com/opensvc/oc3/feeder"
	feederhandlers "github.com/opensvc/oc3/feeder/handlers"
	"github.com/opensvc/oc3/xauth"
)

func startFeeder() error {
	addr := viper.GetString("feeder.addr")
	return listenAndServeFeeder(addr)
}

func listenAndServeFeeder(addr string) error {
	enableUI := viper.GetBool("feeder.ui.enable")

	db, err := newDatabase()
	if err != nil {
		return err
	}

	redisClient := newRedis()

	e := echo.New()
	e.HideBanner = true
	e.HidePort = true

	if viper.GetBool("feeder.pprof.enable") {
		slog.Info("add handler /oc3/feed/api/public/pprof")
		// TODO: move to authenticated path
		pprof.Register(e, "/oc3/feed/api/public/pprof")
	}

	strategy := union.New(
		xauth.NewPublicStrategy("/oc3/feed/api/public/", "/oc3/feed/api/docs", "/oc3/feed/api/version"),
		xauth.NewBasicNode(db),
	)
	if viper.GetBool("feeder.metrics.enable") {
		slog.Info("add handler /oc3/feed/api/public/metrics")
		e.Use(echoprometheus.NewMiddleware("oc3_feeder"))
		e.GET("/oc3/feed/api/public/metrics", echoprometheus.NewHandler())
	}
	e.Use(feederhandlers.AuthMiddleware(strategy))
	slog.Info("register openapi handlers with base url: /oc3/feed/api")
	feeder.RegisterHandlersWithBaseURL(e, &feederhandlers.Api{
		DB:          db,
		Redis:       redisClient,
		UI:          enableUI,
		SyncTimeout: viper.GetDuration("feeder.sync.timeout"),
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
