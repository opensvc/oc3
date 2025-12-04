package scheduler

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type (
	Scheduler struct {
		DB      *sql.DB
		states  map[string]State
		cancels map[string]func()
		sigC    chan os.Signal
	}

	Task struct {
		name   string
		fn     func(context.Context, *Task, *sql.DB) error
		period time.Duration
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
		{
			name:   "refresh_b_action_errors",
			fn:     TaskRefreshBActionErrors,
			period: 24 * time.Hour,
		},
		{
			name:   "trim",
			fn:     TaskTrim,
			period: 24 * time.Hour,
		},
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
		fmt.Println(task.name)
	}
}

func (t TaskList) Get(name string) Task {
	for _, task := range t {
		if task.name == name {
			return task
		}
	}
	return Task{}
}

func (t *Task) IsZero() bool {
	return t.fn == nil
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

func (t *Task) Start(ctx context.Context, db *sql.DB) {
	state, err := t.GetState(ctx, db)
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
			if err := t.SetLastRunAt(ctx, db); err != nil {
				t.Errorf("%s", err)
			}

			// Blocking fn execution, no more timer event until terminated.
			beginAt := time.Now()
			t.Exec(ctx, db)
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

func (t *Task) Exec(ctx context.Context, db *sql.DB) (err error) {
	t.Infof("run")
	status := taskExecStatusOk
	begin := time.Now()

	// Execution
	err = t.fn(ctx, t, db)

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

func (t *Task) GetState(ctx context.Context, db *sql.DB) (State, error) {
	var state State

	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	query := "SELECT last_run_at,is_disabled FROM oc3_scheduler WHERE task_name = ? ORDER BY id DESC LIMIT 1"

	err := db.QueryRowContext(ctx, query, t.name).Scan(&state.LastRunAt, &state.IsDisabled)
	if err == sql.ErrNoRows {
		return state, nil
	}
	return state, err
}

func (t *Task) SetLastRunAt(ctx context.Context, db *sql.DB) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	query := "INSERT INTO oc3_scheduler (task_name, last_run_at) VALUES (?, NOW()) ON DUPLICATE KEY UPDATE last_run_at = NOW()"

	_, err := db.ExecContext(ctx, query, t.name)
	if err != nil {
		return fmt.Errorf("set %s last run time: %w", t.name, err)
	}
	return nil
}

func (t *Scheduler) Infof(format string, args ...any) {
	slog.Info(fmt.Sprintf("scheduler: "+format, args...))
}

func (t *Scheduler) Warnf(format string, args ...any) {
	slog.Warn(fmt.Sprintf("scheduler: "+format, args...))
}

func (t *Scheduler) Errorf(format string, args ...any) {
	slog.Error(fmt.Sprintf("scheduler: "+format, args...))
}

func (t *Scheduler) Debugf(format string, args ...any) {
	slog.Debug(fmt.Sprintf("scheduler: "+format, args...))
}

func (t *Scheduler) toggleTasks(ctx context.Context, states map[string]State) {
	for _, task := range Tasks {
		storedState, _ := states[task.name]
		cachedState, hasCachedState := t.states[task.name]
		if hasCachedState && cachedState.IsDisabled == storedState.IsDisabled {
			//task.Debugf("%s: cachedState: %v storedState: %v", cachedState, storedState)
			continue
		}

		task := Tasks.Get(task.name)
		cancel, hasCancel := t.cancels[task.name]
		switch {
		case storedState.IsDisabled && hasCancel:
			task.Infof("stop")
			cancel()
			delete(t.cancels, task.name)
		case !storedState.IsDisabled && !hasCancel:
			ctx2, cancel := context.WithCancel(ctx)
			t.cancels[task.name] = cancel
			go func() {
				task.Start(ctx2, t.DB)
			}()
		}
	}
	t.states = states
}

func (t *Scheduler) monitor() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	do := func() {
		states, err := t.GetStateMap(ctx)
		if err != nil {
			t.Errorf("states: %s", err)
			return
		} else {
			t.Debugf("states: %v", states)
		}
		t.toggleTasks(ctx, states)
	}

	do()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			do()
		case <-t.sigC:
			cancel()
		}
	}
	return nil
}

func (t *Scheduler) GetStateMap(ctx context.Context) (map[string]State, error) {
	states := make(map[string]State)

	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	query := "SELECT task_name,last_run_at,is_disabled FROM oc3_scheduler"

	result, err := t.DB.QueryContext(ctx, query)
	if err != nil {
		return states, err
	}
	if result == nil {
		return states, nil
	}
	for result.Next() {
		var state State
		var name string
		err := result.Scan(&name, &state.LastRunAt, &state.IsDisabled)
		if err != nil {
			return states, fmt.Errorf("scan: %w", err)
		}
		states[name] = state
	}
	return states, nil
}

func (t *Scheduler) Run() error {
	t.states = make(map[string]State)
	t.cancels = make(map[string]func())
	t.sigC = make(chan os.Signal, 1)

	signal.Notify(t.sigC, os.Interrupt, syscall.SIGTERM)

	t.monitor()

	return nil
}
