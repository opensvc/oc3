package main

import (
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/viper"

	"github.com/opensvc/oc3/oc2websocket"
	"github.com/opensvc/oc3/worker"
)

func work(runners int, queues []string) error {
	db, err := newDatabase()
	if err != nil {
		return err
	}
	if runners < viper.GetInt("worker.runners") {
		runners = viper.GetInt("worker.runners")
	}
	w := &worker.Worker{
		Redis:  newRedis(),
		DB:     db,
		Queues: queues,
		WithTx: viper.GetBool("feeder.tx"),
		Ev: &oc2websocket.T{
			Url: viper.GetString("websocket.url"),
			Key: []byte(viper.GetString("websocket.key")),
		},
		Runners: runners,
	}
	if viper.GetBool("worker.pprof.enable") {
		if p := viper.GetString("worker.pprof.uxsocket"); p != "" {
			if err := pprofUx(p); err != nil {
				return err
			}
		}
		if addr := viper.GetString("worker.pprof.addr"); addr != "" {
			if err := pprofInet(addr); err != nil {
				return err
			}
		}
	}
	if viper.GetBool("worker.metrics.enable") {
		addr := viper.GetString("worker.metrics.addr")
		slog.Info(fmt.Sprintf("metrics listener on http://%s/metrics", addr))
		http.Handle("/metrics", promhttp.Handler())
		go func() {
			_ = http.ListenAndServe(addr, nil)
		}()
	}
	return w.Run()
}
