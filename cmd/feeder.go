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
	pathSpec    = "/api/openapi.json"
	pathPprof   = "/api/pprof"
	pathMetric  = "/api/metrics"
	pathVersion = "/api/version"
	pathDoc     = "/api/docs"
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

	// get enabled features
	enableUI := viper.GetBool("feeder.ui.enable")
	enableMetrics := viper.GetBool("feeder.metrics.enable")
	enablePprof := viper.GetBool("feeder.pprof.enable")

	// define public paths
	publics := []string{pathVersion}
	if enableUI {
		publics = append(publics, pathDoc, pathSpec)
	}
	if enableMetrics {
		publics = append(publics, pathMetric)
	}
	if enablePprof {
		publics = append(publics, pathPprof)
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
		slog.Info(fmt.Sprintf("add handler for profiling: %s", pathPprof))
		pprof.Register(e, pathPprof)
		e.GET(pathPprof, func(c echo.Context) error {
			return c.Redirect(http.StatusMovedPermanently, pathPprof+"/")
		})
	}

	if enableMetrics {
		// TODO: move to authenticated path
		slog.Info(fmt.Sprintf("add handler for metrics: %s", pathMetric))
		e.Use(echoprometheus.NewMiddleware("oc3_feeder"))
		e.GET(pathMetric, echoprometheus.NewHandler())
	}

	if enableUI {
		slog.Info(fmt.Sprintf("add handler for documentation ui: %s", pathDoc))
		g := e.Group(pathDoc)
		g.Use(feederhandlers.UIMiddleware(context.Background(), pathDoc, pathSpec))
		e.GET(pathDoc, func(c echo.Context) error {
			return c.Redirect(http.StatusMovedPermanently, pathDoc+"/")
		})
	}

	slog.Info("listen on " + addr)
	return e.Start(addr)
}
