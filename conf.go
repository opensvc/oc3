package main

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/spf13/viper"

	"github.com/opensvc/oc3/oc2websocket"
)

var (
	configCandidateDirs = []string{"/etc/oc3/", "$HOME/.config/oc3", "./"}
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
	viper.SetDefault("listener_feed.addr", "127.0.0.1:8080")
	viper.SetDefault("listener_feed.pprof.enable", false)
	viper.SetDefault("listener_feed.metrics.enable", false)
	viper.SetDefault("listener_feed.ui.enable", false)
	viper.SetDefault("listener_feed.sync.timeout", "2s")
	viper.SetDefault("listener_api.addr", "127.0.0.1:8081")
	viper.SetDefault("listener_api.pprof.enable", false)
	viper.SetDefault("listener_api.metrics.enable", false)
	viper.SetDefault("listener_api.ui.enable", false)
	viper.SetDefault("listener_api.sync.timeout", "2s")
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
	viper.SetDefault("worker.pprof.uxsocket", "/var/run/oc3_worker_pprof.sock")
	//viper.SetDefault("worker.pprof.addr", "127.0.0.1:9999")
	viper.SetDefault("worker.pprof.enable", false)
	viper.SetDefault("worker.metrics.enable", false)
	viper.SetDefault("worker.metrics.addr", "127.0.0.1:2112")
	viper.SetDefault("scheduler.pprof.uxsocket", "/var/run/oc3_scheduler_pprof.sock")
	//viper.SetDefault("scheduler.pprof.addr", "127.0.0.1:9998")
	viper.SetDefault("scheduler.pprof.enable", false)
	viper.SetDefault("scheduler.metrics.enable", false)
	viper.SetDefault("scheduler.metrics.addr", "127.0.0.1:2111")
	viper.SetDefault("scheduler.task.trim.retention", 365)
	viper.SetDefault("scheduler.task.trim.batch_size", 1000)

	// config file
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	for _, d := range configCandidateDirs {
		viper.AddConfigPath(d)
	}
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return err
		} else {
			slog.Info(err.Error())
		}
	}
	return nil
}

func newEv() *oc2websocket.T {
	return &oc2websocket.T{
		Url: viper.GetString("websocket.url"),
		Key: []byte(viper.GetString("websocket.key")),
	}
}
