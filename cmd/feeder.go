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
	"github.com/opensvc/oc3/feeder/handlers"
	"github.com/opensvc/oc3/xauth"
)

func startFeeder() error {
	addr := viper.GetString("feeder.addr")
	return listenAndServeFeeder(addr)
}

func listenAndServeFeeder(addr string) error {
	const (
		pathApi    = "/api"
		pathSpec   = "openapi.json"
		pathPprof  = "/pprof"
		pathMetric = "/metrics"
	)

	db, err := newDatabase()
	if err != nil {
		return err
	}

	endingSlash := func(s string) string { return strings.TrimSuffix(s, "/") + "/" }
	relPath := func(s string) string { return "./" + strings.TrimPrefix(s, "/") }

	// get enabled features
	enableUI := viper.GetBool("feeder.ui.enable")
	enableMetrics := viper.GetBool("feeder.metrics.enable")
	enablePprof := viper.GetBool("feeder.pprof.enable")

	// define public paths
	publicPath := []string{pathApi + "/version"}
	publicPrefix := []string{}
	if enableUI {
		publicPath = append(publicPath, pathApi)
		for _, p := range []string{"", pathSpec, "swagger-ui.css", "swagger-ui-bundle.js", "swagger-ui-standalone-preset.js"} {
			publicPath = append(publicPath, pathApi+"/"+p)
		}
	}
	if enableMetrics {
		publicPath = append(publicPath, pathMetric)
	}
	if enablePprof {
		publicPrefix = append(publicPrefix, pathPprof)
	}
	slog.Info(fmt.Sprintf("public paths: %s", strings.Join(publicPath, ", ")))
	slog.Info(fmt.Sprintf("public path prefixes: %s", strings.Join(publicPrefix, ", ")))

	// define auth middleware
	authMiddleware := feederhandlers.AuthMiddleware(union.New(
		xauth.NewPublicStrategy(publicPath, publicPrefix),
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
			return c.Redirect(http.StatusMovedPermanently, endingSlash(relPath(pathPprof)))
		})
	}

	if enableMetrics {
		// TODO: move to authenticated path
		slog.Info(fmt.Sprintf("add handler for metrics: %s", pathMetric))
		e.Use(echoprometheus.NewMiddleware("oc3_feeder"))
		e.GET(pathMetric, echoprometheus.NewHandler())
	}

	if enableUI {
		slog.Info(fmt.Sprintf("add handler for documentation ui: %s", pathApi))
		g := e.Group(pathApi)
		g.Use(feederhandlers.UIMiddleware(context.Background(), pathApi, pathSpec))
		e.GET(pathApi, func(c echo.Context) error {
			return c.Redirect(http.StatusMovedPermanently, endingSlash(relPath(pathApi)))
		})
	}

	slog.Info("listen on " + addr)
	return e.Start(addr)
}
