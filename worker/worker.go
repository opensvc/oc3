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
		Redis *redis.Client
		DB    *sql.DB

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
		PrepareDB(ctx context.Context, db *sql.DB, ev EventPublisher, withTx bool) error
	}

	RedisSetter interface {
		SetRedis(r *redis.Client)
	}

	EvSetter interface {
		SetEv(ev EventPublisher)
	}

	JobRunner interface {
		Operationer

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
	slog.Info(fmt.Sprintf("tx enabled: %v", w.WithTx))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	jobC := make(chan []string, w.Runners)
	runSlots := make(chan bool, w.Runners)
	go func(c <-chan []string) {
		for {
			select {
			case j := <-c:
				runSlots <- true
				go func(j []string) {
					defer func() { <-runSlots }()
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
	withTx := w.WithTx
	switch unqueuedJob[0] {
	case cachekeys.FeedDaemonPingQ:
		j = newDaemonPing(unqueuedJob[1])
	case cachekeys.FeedDaemonStatusQ:
		j = newDaemonStatus(unqueuedJob[1])
	case cachekeys.FeedInstanceResourceInfoQ:
		objectName, nodeID, ClusterID, err := w.jobToInstanceAndClusterID(unqueuedJob[1])
		if err != nil {
			err := fmt.Errorf("invalid feed instance resource info index: %w", err)
			slog.Warn(err.Error())
			return err
		}
		j = newjobFeedInstanceResourceInfo(objectName, nodeID, ClusterID)
	case cachekeys.FeedNodeDiskQ:
		// expected unqueuedJob[1]: <nodename>@<nodeID>@<clusterID>
		l := strings.Split(unqueuedJob[1], "@")
		if len(l) != 3 || l[0] == "" || l[1] == "" || l[2] == "" {
			slog.Warn(fmt.Sprintf("invalid feed node disk index expected `nodename`@`nodeID`@`clusterID` found: %s", unqueuedJob[1]))
			return fmt.Errorf("invalid feed node disk index expected `nodename`@`nodeID`@`clusterID` found: %s", unqueuedJob[1])
		}
		j = newNodeDisk(l[0], l[1], l[2])
	case cachekeys.FeedObjectConfigQ:
		objectName, nodeID, ClusterID, err := w.jobToInstanceAndClusterID(unqueuedJob[1])
		if err != nil {
			err := fmt.Errorf("invalid feed instance config index: %s", unqueuedJob[1])
			slog.Warn(err.Error())
			return err
		}
		j = newFeedObjectConfig(objectName, nodeID, ClusterID)
	case cachekeys.FeedSystemQ:
		j = newDaemonSystem(unqueuedJob[1])
	case cachekeys.FeedInstanceStatusQ:
		objectName, nodeID, clusterID, err := w.jobToInstanceAndClusterID(unqueuedJob[1])
		if err != nil {
			err := fmt.Errorf("invalid feed instance status index: %s", unqueuedJob[1])
			slog.Warn(err.Error())
			return err
		}
		j = newInstanceStatus(objectName, nodeID, clusterID)

	case cachekeys.FeedInstanceActionQ:
		objectName, nodeID, ClusterID, uuid, err := w.jobToInstanceClusterIdAndUuid(unqueuedJob[1])
		if err != nil {
			err := fmt.Errorf("invalid feed begin action index: %s", unqueuedJob[1])
			slog.Warn(err.Error())
			return err
		}
		j = newAction(objectName, nodeID, ClusterID, uuid)

	default:
		slog.Debug(fmt.Sprintf("ignore queue '%s'", unqueuedJob[0]))
		return nil
	}
	workType := j.Name()

	if a, ok := j.(RedisSetter); ok {
		a.SetRedis(w.Redis)
	}

	if a, ok := j.(PrepareDBer); ok {
		if err := a.PrepareDB(ctx, w.DB, w.Ev, withTx); err != nil {
			slog.Error(fmt.Sprintf("🔴can't prepare db for %s: %s", workType, err))
			return fmt.Errorf("can't prepare db for %s: %w", workType, err)
		}
	}

	if a, ok := j.(RedisSetter); ok {
		a.SetRedis(w.Redis)
	}

	if a, ok := j.(EvSetter); ok {
		a.SetEv(w.Ev)
	}
	status := operationStatusOk
	err := RunJob(ctx, j)
	duration := time.Since(begin)
	if err != nil {
		status = operationStatusFailed
		slog.Error(fmt.Sprintf("🔴job %s %s: %s", workType, j.Detail(), err))
	}
	processedOperationCounter.With(prometheus.Labels{"desc": workType, "status": status}).Inc()
	operationDuration.With(prometheus.Labels{"desc": workType, "status": status}).Observe(duration.Seconds())
	slog.Debug(fmt.Sprintf("BLPOP %s <- %s: %s", unqueuedJob[0], unqueuedJob[1], duration))
	return nil
}

// jobToInstanceAndClusterID splits a jobName string into path, nodeID, and clusterID based on "@" delimiter.
// Returns an error if the format is invalid or elements are empty.
// Expected jobName: foo@<nodeID>@<clusterID>
func (w *Worker) jobToInstanceAndClusterID(jobName string) (path, nodeID, clusterID string, err error) {
	l := strings.Split(jobName, "@")
	if len(l) != 3 || l[0] == "" || l[1] == "" || l[2] == "" {
		err = fmt.Errorf("unexpected job name: %s", jobName)
	} else {
		path = l[0]
		nodeID = l[1]
		clusterID = l[2]
	}
	return
}

func (w *Worker) jobToInstanceClusterIdAndUuid(jobName string) (path, nodeID, clusterID, uuid string, err error) {
	l := strings.Split(jobName, ":")
	if len(l) != 2 {
		err = fmt.Errorf("unexpected job name: %s", jobName)
		return
	}
	if path, nodeID, clusterID, err = w.jobToInstanceAndClusterID(l[0]); err != nil {
		err = fmt.Errorf("unexpected job name: %s", jobName)
		return
	}
	uuid = l[1]
	return
}
