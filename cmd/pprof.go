package cmd

import (
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
)

func pprofUx(p string) error {
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
