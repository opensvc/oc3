package scheduler

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type (
	Scheduler struct {
		DB *sql.DB
	}

	Task struct {
		scheduler *Scheduler
		name      string
		fn        func(context.Context) error
	}
)

const (
	taskExecStatusOk     = "ok"
	taskExecStatusFailed = "failed"
)

var (
	taskExecCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "oc3",
			Subsystem: "scheduler",
			Name:      "task_exec_count",
			Help:      "Task execution counter",
		},
		[]string{"desc", "status"},
	)

	taskExecDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "oc3",
			Subsystem: "scheduler",
			Name:      "task_exec_duration_seconds",
			Help:      "Task execution duration in seconds.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"desc", "status"},
	)
)

func (t *Task) Run(ctx context.Context) {
	slog.Info(fmt.Sprintf("%s: exec", t.name))
	status := taskExecStatusOk
	begin := time.Now()
	err := t.fn(ctx)
	duration := time.Now().Sub(begin)
	if err != nil {
		status = taskExecStatusFailed
		slog.Error(fmt.Sprintf("%s: %s [%s]", t.name, err, duration))
	} else {
		slog.Info(fmt.Sprintf("%s: %s [%s]", t.name, status, duration))
	}
	taskExecCounter.With(prometheus.Labels{"desc": t.name, "status": status}).Inc()
	taskExecDuration.With(prometheus.Labels{"desc": t.name, "status": status}).Observe(duration.Seconds())
}

func (t *Scheduler) NewTask(name string, fn func(context.Context) error) *Task {
	return &Task{
		scheduler: t,
		name:      name,
		fn:        fn,
	}
}

func (t *Scheduler) Run() error {
	ctx := context.Background()

	tmrHourly := time.NewTicker(time.Hour)
	defer tmrHourly.Stop()
	tmrDaily := time.NewTicker(24 * time.Hour)
	defer tmrDaily.Stop()
	tmrWeekly := time.NewTicker(7 * 24 * time.Hour)
	defer tmrWeekly.Stop()

	tmrNow := time.NewTimer(0)

	for {
		select {
		case <-tmrNow.C:
			// Put here what you want to debug upon scheduler startup
			t.NewTask("TaskRefreshBActionErrors", t.TaskRefreshBActionErrors).Run(ctx)
			tmrNow.Stop()
		case <-tmrHourly.C:
			t.NewTask("TaskRefreshBActionErrors", t.TaskRefreshBActionErrors).Run(ctx)
		case <-tmrDaily.C:
		case <-tmrWeekly.C:
		}
	}
	return nil
}
