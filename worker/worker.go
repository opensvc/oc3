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

		// Runners is the maximum number of jobs to run in parallel.
		Runners int
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

func (w *Worker) Run() error {
	slog.Info(fmt.Sprintf("starting %d runners for queues: %s", w.Runners, strings.Join(w.Queues, ", ")))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	jobC := make(chan []string, w.Runners)
	go func(c <-chan []string) {
		for {
			select {
			case j := <-c:
				go func(j []string) {
					if err := w.runJob(j); err != nil {
						slog.Error(err.Error())
					}
				}(j)
			case <-ctx.Done():
				return
			}
		}
	}(jobC)

	for {
		unqueuedCmd := w.Redis.BLPop(context.Background(), 5*time.Second, w.Queues...)
		unqueuedResult, err := unqueuedCmd.Result()
		switch err {
		case nil:
		case redis.Nil:
			continue
		default:
			slog.Error(err.Error())
			time.Sleep(time.Second)
			continue
		}
		jobC <- unqueuedResult
	}
}

func (w *Worker) runJob(unqueuedJob []string) error {
	begin := time.Now()
	var j JobRunner
	slog.Debug(fmt.Sprintf("BLPOP %s -> %s", unqueuedJob[0], unqueuedJob[1]))
	ctx := context.Background()
	switch unqueuedJob[0] {
	case cachekeys.FeedDaemonPingQ:
		j = newDaemonPing(unqueuedJob[1])
	case cachekeys.FeedDaemonStatusQ:
		j = newDaemonStatus(unqueuedJob[1])
	case cachekeys.FeedNodeDiskQ:
		// expected unqueuedJob[1]: <nodename>@<nodeID>@<clusterID>
		l := strings.Split(unqueuedJob[1], "@")
		if len(l) != 3 || l[0] == "" || l[1] == "" || l[2] == "" {
			slog.Warn(fmt.Sprintf("invalid feed node disk index expected `nodename`@`nodeID`@`clusterID` found: %s", unqueuedJob[1]))
			return fmt.Errorf("invalid feed node disk index expected `nodename`@`nodeID`@`clusterID` found: %s", unqueuedJob[1])
		}
		j = newNodeDisk(l[0], l[1], l[2])
	case cachekeys.FeedObjectConfigQ:
		// expected unqueuedJob[1]: foo@<nodeID>@<clusterID>
		l := strings.Split(unqueuedJob[1], "@")
		if len(l) != 3 || l[0] == "" || l[1] == "" || l[2] == "" {
			slog.Warn(fmt.Sprintf("invalid feed instance config index: %s", unqueuedJob[1]))
			return fmt.Errorf("invalid feed instance config index: %s", unqueuedJob[1])
		}
		j = newFeedObjectConfig(l[0], l[1], l[2])
	case cachekeys.FeedSystemQ:
		j = newDaemonSystem(unqueuedJob[1])
	default:
		slog.Debug(fmt.Sprintf("ignore queue '%s'", unqueuedJob[0]))
		return nil
	}
	workType := j.Name()
	if a, ok := j.(PrepareDBer); ok {
		if err := a.PrepareDB(ctx, w.DB, w.WithTx); err != nil {
			slog.Error(fmt.Sprintf("🔴can't get db for %s: %s", workType, err))
			return fmt.Errorf("can't get db for %s: %w", workType, err)
		}
	}

	if a, ok := j.(RedisSetter); ok {
		a.SetRedis(w.Redis)
	}

	if a, ok := j.(EvSetter); ok {
		a.SetEv(w.Ev)
	}
	status := operationStatusOk
	err := RunJob(j)
	duration := time.Now().Sub(begin)
	if err != nil {
		status = operationStatusFailed
		slog.Error(fmt.Sprintf("🔴job %s %s: %s", workType, j.Detail(), err))
	}
	processedOperationCounter.With(prometheus.Labels{"desc": workType, "status": status}).Inc()
	operationDuration.With(prometheus.Labels{"desc": workType, "status": status}).Observe(duration.Seconds())
	slog.Debug(fmt.Sprintf("BLPOP %s <- %s: %s", unqueuedJob[0], unqueuedJob[1], duration))
	return nil
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
