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

	"github.com/opensvc/oc3/cachekeys"
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

	PrepareDBer interface {
		PrepareDB(ctx context.Context, db *sql.DB, withTx bool) error
	}

	RedisSetter interface {
		SetRedis(r *redis.Client)
	}

	EvSetter interface {
		SetEv(ev EventPublisher)
	}

	JobRunner interface {
		Operationer

		DBGetter
		Name() string
		Detail() string
	}
)

const (
	operationStatusOk     = "ok"
	operationStatusFailed = "failed"
)

var (
	processedOperationCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "oc3",
			Subsystem: "worker",
			Name:      "operation_count",
			Help:      "Counter of worker processed feed operations",
		},
		[]string{"desc", "status"},
	)

	operationDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "oc3",
			Subsystem: "worker",
			Name:      "operation_duration_seconds",
			Help:      "feed operation latencies in seconds.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"desc", "status"},
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
		var j JobRunner
		slog.Debug(fmt.Sprintf("BLPOP %s -> %s", result[0], result[1]))
		ctx := context.Background()
		switch result[0] {
		case cachekeys.FeedDaemonPingQ:
			j = newDaemonPing(result[1])
		case cachekeys.FeedDaemonStatusQ:
			j = newDaemonStatus(result[1])
		case cachekeys.FeedSystemQ:
			j = newDaemonSystem(result[1])
		default:
			slog.Debug(fmt.Sprintf("ignore queue '%s'", result[0]))
			continue
		}
		workType := j.Name()
		if a, ok := j.(PrepareDBer); ok {
			if err = a.PrepareDB(ctx, t.DB, t.WithTx); err != nil {
				slog.Error(fmt.Sprintf("can't get db for %s: %s", workType, err))
				continue
			}
		}

		if a, ok := j.(RedisSetter); ok {
			a.SetRedis(t.Redis)
		}

		if a, ok := j.(EvSetter); ok {
			a.SetEv(t.Ev)
		}
		status := operationStatusOk
		err = RunJob(j)
		duration := time.Now().Sub(begin)
		if err != nil {
			status = operationStatusFailed
			slog.Error(err.Error())
		}
		processedOperationCounter.With(prometheus.Labels{"desc": workType, "status": status}).Inc()
		operationDuration.With(prometheus.Labels{"desc": workType, "status": status}).Observe(duration.Seconds())
		slog.Debug(fmt.Sprintf("BLPOP %s <- %s: %s", result[0], result[1], duration))
	}
}

type (
	tableTracker interface {
		tableChanges() []string
		updateTableModified(context.Context, string) error
	}
)

func pushFromTableChanges(ctx context.Context, oDb tableTracker, ev EventPublisher) error {
	for _, tableName := range oDb.tableChanges() {
		slog.Debug(fmt.Sprintf("pushFromTableChanges %s", tableName))
		if err := oDb.updateTableModified(ctx, tableName); err != nil {
			return fmt.Errorf("pushFromTableChanges: %w", err)
		}
		if err := ev.EventPublish(tableName+"_change", nil); err != nil {
			return fmt.Errorf("EventPublish send %s: %w", tableName, err)
		}
	}
	return nil
}
