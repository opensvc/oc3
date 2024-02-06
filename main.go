package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/spf13/viper"
)

func fatal(err error) {
	if err != nil {
		slog.Info(err.Error())
		os.Exit(1)
	}
}

func main() {
	if err := initConf(); err != nil {
		fatal(fmt.Errorf("can't load config: %w", err))
	}
	addr := viper.GetString("Listen")
	if err := listenAndServe(addr); err != nil {
		fatal(err)
	}
}
