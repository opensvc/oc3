package worker

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/opensvc/oc3/cache"
)

type (
	Worker struct {
		Redis  *redis.Client
		DB     *sql.DB
		Queues []string
		WithTx bool
		Ev     EventPublisher
	}

	EventPublisher interface {
		EventPublish(eventName string, data map[string]any) error
	}
)

var (
	promCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "worker_processed_ops_total",
			Help: "The total number of worker processed operations",
		},
		[]string{"operation", "status"},
	)
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
		var workType string
		slog.Debug(fmt.Sprintf("BLPOP %s -> %s", result[0], result[1]))
		switch result[0] {
		case cache.KeySystem:
			workType = "daemonSystem"
			err = t.handleSystem(result[1])
		case cache.KeyDaemonStatus:
			workType = "daemonStatus"
			err = t.handleDaemonStatus(result[1])
		case cache.KeyPackages:
			workType = "daemonPackage"
			err = t.handlePackage(result[1])
		default:
			slog.Debug(fmt.Sprintf("ignore queue '%s'", result[0]))
		}
		status := "success"
		if err != nil {
			status = "failed"
			slog.Error(err.Error())
		}
		promCounter.With(prometheus.Labels{"operation": workType, "status": status}).Inc()
		slog.Debug(fmt.Sprintf("BLPOP %s <- %s: %s", result[0], result[1], time.Now().Sub(begin)))
	}
}
