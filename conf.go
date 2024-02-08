package main

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/spf13/viper"
)

func initConfig() error {
	// env
	viper.SetEnvPrefix("OC3")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	// defaults
	viper.SetDefault("listen", "127.0.0.1:8080")
	viper.SetDefault("db.username", "opensvc")
	viper.SetDefault("db.host", "127.0.0.1")
	viper.SetDefault("db.port", "3306")
	viper.SetDefault("db.log.level", "warn")
	viper.SetDefault("db.log.slow_query_threshold", "1s")
	viper.SetDefault("redis.db", 0)
	viper.SetDefault("redis.address", "localhost:6379")
	viper.SetDefault("redis.password", "")

	// config file
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	slog.Info(fmt.Sprintf("add candidate config directory: %s", "/etc/oc3/"))
	viper.AddConfigPath("/etc/oc3")
	slog.Info(fmt.Sprintf("add candidate config directory: %s", "$HOME/.oc3/"))
	viper.AddConfigPath("$HOME/.oc3")
	slog.Info(fmt.Sprintf("add candidate config directory: %s", "./"))
	viper.AddConfigPath(".")
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return err
		} else {
			slog.Info(err.Error())
		}
	}
	slog.Info(fmt.Sprintf("using config file: %s", viper.ConfigFileUsed()))
	return nil
}
