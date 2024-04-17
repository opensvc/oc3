package main

import (
	"fmt"
	"log/slog"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

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
		if p := viper.GetString("worker.pprof.uxsocket"); p != "" {
			if err := workerUxPprof(p); err != nil {
				return err
			}
		}
		if addr := viper.GetString("worker.pprof.addr"); addr != "" {
			if err := workerHttpPprof(addr); err != nil {
				return err
			}
		}
	}
	return w.Run()
}

func workerUxPprof(p string) error {
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

func workerHttpPprof(addr string) error {
	slog.Info(fmt.Sprintf("pprof listener on %s", addr))
	c := make(chan any)
	go func() {
		err := http.ListenAndServe(addr, nil)
		slog.Info(fmt.Sprintf("pprof listener on %s", addr))
		select {
		case c <- err:
		default:
		}
	}()
	select {
	case i := <-c:
		err, ok := i.(error)
		if ok {
			return err
		}
	case <-time.After(100 * time.Millisecond):
		// don't wait for future errors
		return nil
	}
	return nil
}
