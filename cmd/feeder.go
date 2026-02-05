package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/labstack/echo-contrib/echoprometheus"
	"github.com/labstack/echo-contrib/pprof"
	"github.com/labstack/echo/v4"
	"github.com/shaj13/go-guardian/v2/auth/strategies/union"
	"github.com/spf13/viper"

	"github.com/opensvc/oc3/feeder"
	feederhandlers "github.com/opensvc/oc3/feeder/handlers"
	"github.com/opensvc/oc3/xauth"
)

const (
	pathApi     = "/api"
	pathSpec    = "openapi.json"
	pathPprof   = "pprof"
	pathMetric  = "metrics"
	pathVersion = "version"
	pathDoc     = "docs"
)

func startFeeder() error {
	addr := viper.GetString("feeder.addr")
	return listenAndServeFeeder(addr)
}

func listenAndServeFeeder(addr string) error {
	db, err := newDatabase()
	if err != nil {
		return err
	}

	fromPath := func(p string) string { return fmt.Sprintf("%s/%s", pathApi, p) }
	endingSlash := func(s string) string { return strings.TrimSuffix(s, "/") + "/" }

	// get enabled features
	enableUI := viper.GetBool("feeder.ui.enable")
	enableMetrics := viper.GetBool("feeder.metrics.enable")
	enablePprof := viper.GetBool("feeder.pprof.enable")

	// define public paths
	publics := []string{fromPath(pathVersion)}
	if enableUI {
		publics = append(publics, fromPath(pathDoc), fromPath(pathSpec))
	}
	if enableMetrics {
		publics = append(publics, fromPath(pathMetric))
	}
	if enablePprof {
		publics = append(publics, fromPath(pathPprof))
	}
	slog.Info(fmt.Sprintf("public paths: %s", strings.Join(publics, ", ")))

	// define auth middleware
	authMiddleware := feederhandlers.AuthMiddleware(union.New(
		xauth.NewPublicStrategy(publics...),
		xauth.NewBasicNode(db),
	))

	e := echo.New()
	e.HideBanner = true
	e.HidePort = true

	e.Use(authMiddleware)

	slog.Info(fmt.Sprintf("add handler for openapi: %s", pathApi))
	feeder.RegisterHandlersWithBaseURL(e, &feederhandlers.Api{
		DB:          db,
		Redis:       newRedis(),
		UI:          enableUI,
		SyncTimeout: viper.GetDuration("feeder.sync.timeout"),
	}, pathApi)

	if enablePprof {
		// TODO: move to authenticated path
		s := fromPath(pathPprof)
		slog.Info(fmt.Sprintf("add handler for profiling: %s", s))
		pprof.Register(e, s)
		e.GET(s, func(c echo.Context) error {
			return c.Redirect(http.StatusMovedPermanently, endingSlash(pathPprof))
		})
	}

	if enableMetrics {
		// TODO: move to authenticated path
		slog.Info(fmt.Sprintf("add handler for metrics: %s", pathMetric))
		e.Use(echoprometheus.NewMiddleware("oc3_feeder"))
		e.GET(fromPath(pathMetric), echoprometheus.NewHandler())
	}

	if enableUI {
		s := fromPath(pathDoc)
		slog.Info(fmt.Sprintf("add handler for documentation ui: %s", s))
		g := e.Group(s)
		g.Use(feederhandlers.UIMiddleware(context.Background(), s, "../"+pathSpec))
		e.GET(s, func(c echo.Context) error {
			return c.Redirect(http.StatusMovedPermanently, endingSlash(pathDoc))
		})
	}

	slog.Info("listen on " + addr)
	return e.Start(addr)
}
