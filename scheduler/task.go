package scheduler

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/opensvc/oc3/cdb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type (
	Task struct {
		name   string
		period time.Duration
		fn     func(context.Context, *Task) error

		db      *sql.DB
		ev      eventPublisher
		session *cdb.Session
	}

	TaskList []Task

	State struct {
		IsDisabled bool
		LastRunAt  time.Time
	}
)

const (
	taskExecStatusOk     = "ok"
	taskExecStatusFailed = "failed"
)

var (
	Tasks = TaskList{
		TaskRefreshBActionErrors,
		TaskTrim,
		TaskScrub,
	}

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

func (t TaskList) Print() {
	for _, task := range t {
		fmt.Println(task.Name())
	}
}

func (t TaskList) Get(name string) Task {
	for _, task := range t {
		if task.Name() == name {
			return task
		}
	}
	return Task{}
}

func (t *Task) IsZero() bool {
	return t.fn == nil
}

func (t *Task) SetEv(ev eventPublisher) {
	t.ev = ev
}

func (t *Task) SetDB(db *sql.DB) {
	t.db = db
}

func (t *Task) Name() string {
	return t.name
}

func (t *Task) Session() *cdb.Session {
	if t.session == nil {
		t.session = cdb.NewSession(t.db, t.ev)
	}
	return t.session
}

func (t *Task) Infof(format string, args ...any) {
	slog.Info(fmt.Sprintf(t.name+": "+format, args...))
}

func (t *Task) Warnf(format string, args ...any) {
	slog.Warn(fmt.Sprintf(t.name+": "+format, args...))
}

func (t *Task) Errorf(format string, args ...any) {
	slog.Error(fmt.Sprintf(t.name+": "+format, args...))
}

func (t *Task) Debugf(format string, args ...any) {
	slog.Debug(fmt.Sprintf(t.name+": "+format, args...))
}

func (t *Task) Start(ctx context.Context) {
	state, err := t.GetState(ctx)
	if err != nil {
		t.Errorf("%s", err)
	}
	var initialDelay time.Duration
	if !state.LastRunAt.IsZero() {
		initialDelay = state.LastRunAt.Add(t.period).Sub(time.Now())
	} else {
		initialDelay = 0
	}

	if initialDelay < 0 {
		initialDelay = 0
	}

	t.Infof("start with period=%s, last was %s, next in %s", t.period, state.LastRunAt.Format(time.RFC3339), initialDelay)
	timer := time.NewTimer(initialDelay)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			// Update the last run time persistant store
			if err := t.SetLastRunAt(ctx); err != nil {
				t.Errorf("%s", err)
			}

			// Blocking fn execution, no more timer event until terminated.
			beginAt := time.Now()
			_ = t.Exec(ctx)
			endAt := time.Now()

			// Plan the next execution, correct the drift
			nextPeriod := beginAt.Add(t.period).Sub(endAt)
			if nextPeriod < 0 {
				nextPeriod = time.Second
			}
			timer.Reset(nextPeriod)
		}
	}
}

func (t *Task) Exec(ctx context.Context) (err error) {
	t.Infof("run")
	status := taskExecStatusOk
	begin := time.Now()

	// Execution
	err = t.fn(ctx, t)

	duration := time.Now().Sub(begin)
	if err != nil {
		status = taskExecStatusFailed
		t.Errorf("%s [%s]", err, duration)
	} else {
		t.Infof("%s [%s]", status, duration)
	}
	taskExecCounter.With(prometheus.Labels{"desc": t.name, "status": status}).Inc()
	taskExecDuration.With(prometheus.Labels{"desc": t.name, "status": status}).Observe(duration.Seconds())
	return
}

func (t *Task) GetState(ctx context.Context) (State, error) {
	var state State

	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	query := "SELECT last_run_at,is_disabled FROM oc3_scheduler WHERE task_name = ? ORDER BY id DESC LIMIT 1"

	err := t.db.QueryRowContext(ctx, query, t.name).Scan(&state.LastRunAt, &state.IsDisabled)
	if err == sql.ErrNoRows {
		return state, nil
	}
	return state, err
}

func (t *Task) SetLastRunAt(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	query := "INSERT INTO oc3_scheduler (task_name, last_run_at) VALUES (?, NOW()) ON DUPLICATE KEY UPDATE last_run_at = NOW()"

	_, err := t.db.ExecContext(ctx, query, t.name)
	if err != nil {
		return fmt.Errorf("set %s last run time: %w", t.name, err)
	}
	return nil
}
