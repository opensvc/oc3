package cmd

import (
	"fmt"
	"log/slog"

	"github.com/go-redis/redis/v8"
	"github.com/spf13/viper"
)

func newRedis() *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr:     viper.GetString("Redis.Address"),
		Password: viper.GetString("Redis.Password"),
		DB:       viper.GetInt("Redis.Database"),
	})
	slog.Info(fmt.Sprintf("redis addr=%s", client.Options().Addr))
	return client
}
