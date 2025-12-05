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
)

type (
	Scheduler struct {
		DB      *sql.DB
		states  map[string]State
		cancels map[string]func()
		sigC    chan os.Signal
	}
)

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
