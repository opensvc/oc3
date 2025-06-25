package main

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/spf13/viper"
)

var (
	configCandidateDirs = []string{"/etc/oc3/", "$HOME/.oc3/", "./"}
)

func logConfigDir() {
	slog.Info(fmt.Sprintf("candidate config directories: %s", configCandidateDirs))
}

func logConfigFileUsed() {
	slog.Info(fmt.Sprintf("used config file: %s", viper.ConfigFileUsed()))
}

func initConfig() error {
	// env
	viper.SetEnvPrefix("OC3")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	// defaults
	viper.SetDefault("listener.addr", "127.0.0.1:8080")
	viper.SetDefault("listener.pprof.enable", false)
	viper.SetDefault("listener.metrics.enable", false)
	viper.SetDefault("listener.ui.enable", false)
	viper.SetDefault("db.username", "opensvc")
	viper.SetDefault("db.host", "127.0.0.1")
	viper.SetDefault("db.port", "3306")
	viper.SetDefault("db.log.level", "warn")
	viper.SetDefault("db.log.slow_query_threshold", "1s")
	viper.SetDefault("redis.db", 0)
	viper.SetDefault("redis.address", "localhost:6379")
	viper.SetDefault("redis.password", "")
	viper.SetDefault("feeder.tx", true)
	viper.SetDefault("websocket.key", "magix123")
	viper.SetDefault("websocket.url", "http://127.0.0.1:8889")
	viper.SetDefault("worker.runners", 1)
	viper.SetDefault("worker.pprof.uxsocket", "/var/run/oc3_worker.sock")
	viper.SetDefault("worker.pprof.addr", "127.0.0.1:9999")
	viper.SetDefault("worker.pprof.enable", false)
	viper.SetDefault("worker.metrics.enable", false)
	viper.SetDefault("worker.metrics.addr", "127.0.0.1:2112")

	// config file
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("/etc/oc3")
	viper.AddConfigPath("$HOME/.oc3")
	viper.AddConfigPath(".")
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return err
		} else {
			slog.Info(err.Error())
		}
	}
	return nil
}
