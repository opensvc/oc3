package main

import (
	"fmt"
	"log/slog"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"

	"github.com/spf13/viper"

	"github.com/opensvc/oc3/oc2websocket"
	"github.com/opensvc/oc3/worker"
)

func work(queues []string) error {
	db, err := newDatabase()
	if err != nil {
		return err
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
	}
	if err != nil {
		return err
	}
	if viper.GetBool("worker.pprof.enable") {
		if err := workerPprof(viper.GetString("worker.pprof.uxsocket")); err != nil {
			return err
		}
	}
	return w.Run()
}

func workerPprof(p string) error {
	slog.Info(fmt.Sprintf("pprof listener on %s", p))
	if err := os.RemoveAll(p); err != nil {
		return err
	}
	listener, err := net.Listen("unix", p)
	if err != nil {
		return err
	}
	go func() {
		server := http.Server{}
		if err := server.Serve(listener); err != nil {
			slog.Error(fmt.Sprintf("worker ux listener: %s", err))
		}
	}()
	return nil
}
