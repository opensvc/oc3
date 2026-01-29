package cmd

import (
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"
)

func pprofUx(p string) error {
	slog.Info(fmt.Sprintf("pprof ux listener on %s", p))
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
			slog.Error(fmt.Sprintf("pprof ux listener on %s: %s", p, err))
		}
	}()
	return nil
}

func pprofInet(addr string) error {
	slog.Info(fmt.Sprintf("pprof inet listener on %s", addr))
	c := make(chan any)
	go func() {
		err := http.ListenAndServe(addr, nil)
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
