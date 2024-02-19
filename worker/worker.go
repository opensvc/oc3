package worker

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/opensvc/oc3/cache"
)

type (
	Worker struct {
		Redis  *redis.Client
		DB     *sql.DB
		Queues []string
	}
)

func (t *Worker) Run() error {
	slog.Info(fmt.Sprintf("work with queues: %s", strings.Join(t.Queues, ", ")))
	for {
		cmd := t.Redis.BLPop(context.Background(), 5*time.Second, t.Queues...)
		result, err := cmd.Result()
		switch err {
		case nil:
		case redis.Nil:
			continue
		default:
			slog.Error(err.Error())
			time.Sleep(time.Second)
			continue
		}
		begin := time.Now()
		slog.Debug(fmt.Sprintf("BLPOP %s -> %s", result[0], result[1]))
		switch result[0] {
		case cache.KeySystem:
			err = t.handleSystem(result[1])
		case cache.KeyDaemonStatus:
			err = t.handleDaemonStatus(result[1])
		case cache.KeyPackages:
			err = t.handlePackage(result[1])
		default:
			slog.Warn(fmt.Sprintf("unsupported queue: %s", result[0]))
		}
		if err != nil {
			slog.Error(err.Error())
		}
		slog.Debug(fmt.Sprintf("BLPOP %s <- %s: %s", result[0], result[1], time.Now().Sub(begin)))
	}
	return nil
}
