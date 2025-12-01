package main

import (
	"fmt"
	"log/slog"
	"net/http"

	"github.com/opensvc/oc3/scheduler"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/viper"
)

func schedule() error {
	if err := setup(); err != nil {
		return err
	}
	db, err := newDatabase()
	if err != nil {
		return err
	}
	if viper.GetBool("scheduler.pprof.enable") {
		if p := viper.GetString("scheduler.pprof.uxsocket"); p != "" {
			if err := pprofUx(p); err != nil {
				return err
			}
		}
		if addr := viper.GetString("scheduler.pprof.addr"); addr != "" {
			if err := pprofInet(addr); err != nil {
				return err
			}
		}
	}
	if viper.GetBool("scheduler.metrics.enable") {
		addr := viper.GetString("scheduler.metrics.addr")
		slog.Info(fmt.Sprintf("metrics listener on http://%s/metrics", addr))
		http.Handle("/metrics", promhttp.Handler())
		go func() {
			_ = http.ListenAndServe(addr, nil)
		}()
	}
	sched := &scheduler.Scheduler{
		DB: db,
	}
	return sched.Run()
}
