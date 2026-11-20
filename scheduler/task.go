package scheduler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/spf13/viper"

	"github.com/opensvc/oc3/cdb"
)

type (
	TaskFunc func(context.Context, *Task) error

	Task struct {
		name     string
		desc     string
		period   time.Duration
		timeout  time.Duration
		fn       TaskFunc
		children TaskList

		db      *sql.DB
		Redis   *redis.Client
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
		TaskSysreport,
		TaskSysreportPurge,
		TaskRefreshBActionErrors,
		TaskAlertUpdateActionErrors,
		TaskUpdateVirtualAssets,
		TaskTrim,
		TaskScrub1M,
		TaskScrub10M,
		TaskScrub1H,
		TaskScrub1D,
		TaskStat1D,
		TaskAlert1M,
		TaskAlert1H,
		TaskAlert1D,
		TaskMetrics,
		TaskServicesAvailability,
		TaskPurgeServices,
		TaskPurgeNodes,
		TaskNotify,
		TaskNotifyJanitor,
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
			// tasks run from sub-second to about an hour
			Buckets: []float64{.1, .5, 1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600},
		},
		[]string{"desc", "status"},
	)
)

func (t TaskList) Print() {
	t.Fprint(os.Stdout)
}

func (t TaskList) Fprint(w io.Writer) {
	for _, task := range t {
		fmt.Fprintln(w, task.name)
		for _, child := range task.children {
			fmt.Fprintln(w, "  "+child.name)
		}
	}
}

func (t TaskList) Get(name string) Task {
	for _, task := range t {
		if task.name == name {
			return task
		}
		for _, child := range task.children {
			if child.name == name {
				return child
			}
		}
	}
	return Task{}
}

func (t *Task) IsZero() bool {
	return t.fn == nil && t.children == nil
}

func (t *Task) SetEv(ev eventPublisher) {
	t.ev = ev
}

func (t *Task) SetDB(db *sql.DB) {
	t.db = db
}

func (t *Task) SetRedis(r *redis.Client) {
	t.Redis = r
}

func (t *Task) Name() string {
	return t.name
}

func (t *Task) DBXRO(ctx context.Context) (*cdb.DB, error) {
	tx, err := t.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, err
	}
	return &cdb.DB{
		Session: cdb.NewSession(tx, t.ev),
		DB:      tx,
		DBLck:   cdb.InitDbLocker(t.db),
	}, nil
}

func (t *Task) DBX(ctx context.Context) (*cdb.DB, error) {
	return t.DB(), nil
	// TODO: cleanup
	//	tx, err := t.db.BeginTx(ctx, nil)
	//	if err != nil {
	//		return nil, err
	//	}
	//	return &cdb.DB{
	//		Session: cdb.NewSession(tx, t.ev),
	//		DB:      tx,
	//		DBLck:   cdb.InitDbLocker(t.db),
	//		HasTx:   true,
	//	}, nil
}

func (t *Task) DB() *cdb.DB {
	d := cdb.New(t.db)
	d.CreateSession(t.ev)
	return d
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
	t.Debugf("run")
	status := taskExecStatusOk
	begin := time.Now()

	// Execution
	if t.fn != nil {
		fnCtx, cancel := context.WithTimeout(ctx, t.timeout)
		err = t.fn(fnCtx, t)
		cancel()
	}

	// Each child runs under its own timeout, not the rest of the parent's: a
	// child slow to the end of it would leave the next ones no time at all.
	for _, child := range t.children {
		child.db = t.db
		child.ev = t.ev
		child.session = t.session
		child.name = fmt.Sprintf("%s: %s", t.name, child.name)
		err = errors.Join(err, child.Exec(ctx))
	}

	duration := time.Since(begin)
	if err != nil {
		status = taskExecStatusFailed
		t.Errorf("%s [%s]", err, duration)
	} else {
		t.Infof("%s [%s]", status, duration)
	}
	taskExecCounter.With(prometheus.Labels{"desc": t.name, "status": status}).Inc()
	taskExecDuration.With(prometheus.Labels{"desc": t.name, "status": status}).Observe(duration.Seconds())
	if rerr := t.recordRun(ctx, status, duration, err); rerr != nil {
		t.Warnf("record the run: %s", rerr)
	}
	return
}

// runRetention is how long the runs of the tasks are kept in
// oc3_scheduler_runs.
const runRetention = "7 DAY"

// runErrorMaxLen is the length the error of a run is cut to.
const runErrorMaxLen = 4000

// recordRun records a run of the task, for the scheduler tasks page: on its
// oc3_scheduler row, the last run, and in oc3_scheduler_runs, where the runs
// older than runRetention are dropped. A run stopped by the end of ctx is
// recorded all the same.
func (t *Task) recordRun(ctx context.Context, status string, duration time.Duration, runErr error) error {
	if t.db == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	var errText sql.NullString
	if runErr != nil {
		errText.String, errText.Valid = runErr.Error(), true
		if len(errText.String) > runErrorMaxLen {
			errText.String = errText.String[:runErrorMaxLen]
		}
	}
	ms := duration.Milliseconds()
	// The begin is computed by the database, as the other dates of the table,
	// from its clock.
	const beginAt = "NOW(3) - INTERVAL ? * 1000 MICROSECOND"
	if _, err := t.db.ExecContext(ctx, "INSERT INTO oc3_scheduler_runs (task_name, begin_at, duration_ms, status, error)"+
		" VALUES (?, "+beginAt+", ?, ?, ?)", t.name, ms, ms, status, errText); err != nil {
		return err
	}
	if _, err := t.db.ExecContext(ctx, "INSERT INTO oc3_scheduler (task_name, last_status, last_begin_at, last_duration_ms, last_error)"+
		" VALUES (?, ?, "+beginAt+", ?, ?) ON DUPLICATE KEY UPDATE last_status = VALUES(last_status),"+
		" last_begin_at = VALUES(last_begin_at), last_duration_ms = VALUES(last_duration_ms), last_error = VALUES(last_error)",
		t.name, status, ms, ms, errText); err != nil {
		return err
	}
	_, err := t.db.ExecContext(ctx, "DELETE FROM oc3_scheduler_runs WHERE task_name = ? AND begin_at < NOW() - INTERVAL "+runRetention, t.name)
	return err
}

// declaredTask is a task or a subtask as the scheduler declares it in
// oc3_scheduler.
type declaredTask struct {
	name, parent, desc string
	period, timeout    time.Duration
}

// declared returns the tasks of the list and their subtasks, the subtasks
// named "<task>: <subtask>" as Exec names them.
func (t TaskList) declared() []declaredTask {
	var l []declaredTask
	for _, task := range t {
		l = append(l, declaredTask{name: task.name, desc: task.desc, period: task.period, timeout: task.timeout})
		for _, child := range task.children {
			l = append(l, declaredTask{
				name:    task.name + ": " + child.name,
				parent:  task.name,
				desc:    child.desc,
				period:  task.period,
				timeout: child.timeout,
			})
		}
	}
	return l
}

// Declare writes the tasks of the list and their subtasks in oc3_scheduler,
// with their description, period and timeout, and marks the rows of the
// tasks the list does not hold any more as not declared.
func (t TaskList) Declare(ctx context.Context, db *sql.DB) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	l := t.declared()
	names := make([]any, len(l))
	for i, d := range l {
		names[i] = d.name
		if _, err := db.ExecContext(ctx, "INSERT INTO oc3_scheduler (task_name, parent, task_desc, period_s, timeout_s, declared)"+
			" VALUES (?, ?, ?, ?, ?, 1) ON DUPLICATE KEY UPDATE parent = VALUES(parent), task_desc = VALUES(task_desc),"+
			" period_s = VALUES(period_s), timeout_s = VALUES(timeout_s), declared = 1",
			d.name, d.parent, d.desc, int64(d.period.Seconds()), int64(d.timeout.Seconds())); err != nil {
			return fmt.Errorf("declare %s: %w", d.name, err)
		}
	}
	if len(names) == 0 {
		return nil
	}
	if _, err := db.ExecContext(ctx, "UPDATE oc3_scheduler SET declared = 0 WHERE task_name NOT IN ("+
		strings.TrimSuffix(strings.Repeat("?,", len(names)), ",")+")", names...); err != nil {
		return fmt.Errorf("undeclare the tasks gone: %w", err)
	}
	return nil
}

func (t *Task) GetState(ctx context.Context) (State, error) {
	var state State

	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	query := "SELECT last_run_at,is_disabled FROM oc3_scheduler WHERE task_name = ? ORDER BY id DESC LIMIT 1"

	// A task declared at the start of the scheduler and never run has no last
	// run.
	var lastRunAt sql.NullTime
	var isDisabled sql.NullBool
	err := t.db.QueryRowContext(ctx, query, t.name).Scan(&lastRunAt, &isDisabled)
	if err == sql.ErrNoRows {
		return state, nil
	}
	state.LastRunAt, state.IsDisabled = lastRunAt.Time, isDisabled.Bool
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

// maxAge returns the scheduler.task.<name>.max_age setting: the age after
// which the task considers a value outdated.
func maxAge(name string) (time.Duration, error) {
	return parseMaxAgeKey("scheduler.task." + name + ".max_age")
}

// parseMaxAgeKey parses the key value with cdb.ParseMaxAge, accepting the
// time.ParseDuration units plus "d", and rejects values < 1m.
func parseMaxAgeKey(key string) (time.Duration, error) {
	s := viper.GetString(key)
	d, err := cdb.ParseMaxAge(s)
	if err != nil {
		return 0, fmt.Errorf("invalid %s value %q: %w", key, s, err)
	}
	if d < time.Minute {
		return 0, fmt.Errorf("invalid %s value %q: must be >= 1m", key, s)
	}
	return d, nil
}

// maxAgeParsers are the parsers of the max_age settings not parsed by
// parseMaxAgeKey, the task accepting other values, like 0 disabling the
// purge_services age based purge and the purge_nodes purge, their default.
var maxAgeParsers = map[string]func() (time.Duration, error){
	"scheduler.task.purge_services.max_age": purgeServicesMaxAge,
	"scheduler.task.purge_nodes.max_age":    purgeNodesMaxAge,
}

// ValidateMaxAges verifies all the scheduler.task.<name>.max_age settings,
// so a bad value is reported at startup instead of when the task runs. Each
// setting is verified by the parser of its task.
func ValidateMaxAges() error {
	var errs []error
	for _, key := range viper.AllKeys() {
		if !strings.HasPrefix(key, "scheduler.task.") || !strings.HasSuffix(key, ".max_age") {
			continue
		}
		parse := func() (time.Duration, error) { return parseMaxAgeKey(key) }
		if p, ok := maxAgeParsers[key]; ok {
			parse = p
		}
		if _, err := parse(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
