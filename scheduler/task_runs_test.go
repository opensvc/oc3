package scheduler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/opensvc/oc3/migrate"
)

// legacyOc3Scheduler is oc3_scheduler as the collector created it, before
// the migrations.
const legacyOc3Scheduler = "CREATE TABLE oc3_scheduler (id int(11) NOT NULL AUTO_INCREMENT, task_name varchar(256) DEFAULT NULL," +
	" is_disabled tinyint(1) DEFAULT 0, last_run_at datetime DEFAULT NULL, PRIMARY KEY (id), UNIQUE KEY uk (task_name))" +
	" DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci"

// schedulerTestDB is a new database of the MariaDB server OC3_TEST_MYSQL_DSN
// names, holding oc3_scheduler migrated by 0039_scheduler_runs, dropped at
// the end of the test.
func schedulerTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("OC3_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("OC3_TEST_MYSQL_DSN is not set, as root:pass@tcp(127.0.0.1:3306)/: no MariaDB to test against")
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ParseTime = true
	cfg.InterpolateParams = true
	cfg.DBName = ""
	admin, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	name := fmt.Sprintf("oc3_test_scheduler_%d", time.Now().UnixNano())
	if _, err := admin.Exec("CREATE DATABASE " + name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec("DROP DATABASE " + name) })
	cfg.DBName = name
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	statements := []string{legacyOc3Scheduler}
	all, err := migrate.All()
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range all {
		if m.Name == "scheduler_runs" {
			statements = append(statements, m.Statements...)
		}
	}
	for _, s := range statements {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	return db
}

// TestDeclareAndRecordRuns checks the scheduler declares its tasks and
// subtasks, marks those gone, and records each run of a task and of its
// subtasks, its last run on the task row and all of them in
// oc3_scheduler_runs.
func TestDeclareAndRecordRuns(t *testing.T) {
	db := schedulerTestDB(t)
	ctx := context.Background()
	if _, err := db.Exec("INSERT INTO oc3_scheduler (task_name, is_disabled, last_run_at) VALUES ('gone', 0, NOW()), ('t1', 1, NOW())"); err != nil {
		t.Fatal(err)
	}
	fail := errors.New(strings.Repeat("x", runErrorMaxLen+10))
	tasks := TaskList{
		{name: "t1", desc: "first", period: time.Hour, timeout: time.Minute, children: TaskList{
			{name: "ok", desc: "a subtask", timeout: time.Second, fn: func(context.Context, *Task) error { return nil }},
			{name: "ko", timeout: 2 * time.Second, fn: func(context.Context, *Task) error { return fail }},
		}},
	}
	if err := tasks.Declare(ctx, db); err != nil {
		t.Fatal(err)
	}
	type row struct {
		name, parent, desc string
		period, timeout    int
		disabled, declared bool
	}
	var got []row
	rows, err := db.Query("SELECT task_name, parent, task_desc, period_s, timeout_s, is_disabled, declared FROM oc3_scheduler ORDER BY task_name")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.name, &r.parent, &r.desc, &r.period, &r.timeout, &r.disabled, &r.declared); err != nil {
			t.Fatal(err)
		}
		got = append(got, r)
	}
	_ = rows.Close()
	want := []row{
		{name: "gone"},
		{name: "t1", desc: "first", period: 3600, timeout: 60, disabled: true, declared: true},
		{name: "t1: ko", parent: "t1", period: 3600, timeout: 2, declared: true},
		{name: "t1: ok", parent: "t1", desc: "a subtask", period: 3600, timeout: 1, declared: true},
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("declared\n got %v\nwant %v", got, want)
	}

	// The states read by the scheduler accept the tasks declared and never
	// run, whose last run is NULL.
	sched := &Scheduler{DB: db}
	states, err := sched.GetStateMap(ctx)
	if err != nil {
		t.Fatalf("states: %v", err)
	}
	if !states["t1"].IsDisabled || !states["t1: ok"].LastRunAt.IsZero() || len(states) != 4 {
		t.Errorf("states %v", states)
	}
	task := tasks[0]
	task.db = db
	if st, err := task.GetState(ctx); err != nil || !st.IsDisabled {
		t.Errorf("state of t1: %v, %v", st, err)
	}
	sub := Task{name: "t1: ok", db: db}
	if st, err := sub.GetState(ctx); err != nil || !st.LastRunAt.IsZero() {
		t.Errorf("state of a subtask never run: %v, %v", st, err)
	}
	if err := task.Exec(ctx); !errors.Is(err, fail) {
		t.Fatalf("exec: %v", err)
	}
	var n, failed int
	if err := db.QueryRow("SELECT COUNT(*), SUM(status = 'failed') FROM oc3_scheduler_runs").Scan(&n, &failed); err != nil {
		t.Fatal(err)
	}
	if n != 3 || failed != 2 {
		t.Errorf("%d runs, %d failed, want 3 runs (the task and its 2 subtasks), 2 failed (ko and the task)", n, failed)
	}
	var status, lastErr string
	var begin time.Time
	if err := db.QueryRow("SELECT last_status, last_error, last_begin_at FROM oc3_scheduler WHERE task_name = 't1: ko'").Scan(&status, &lastErr, &begin); err != nil {
		t.Fatal(err)
	}
	if status != "failed" || len(lastErr) != runErrorMaxLen {
		t.Errorf("last run of ko: %s, error of %d bytes", status, len(lastErr))
	}
	if d := time.Since(begin); d < -time.Minute || d > time.Minute {
		t.Errorf("last begin %s, %s ago", begin, d)
	}

	// The runs older than the retention are dropped at the next run.
	if _, err := db.Exec("UPDATE oc3_scheduler_runs SET begin_at = NOW() - INTERVAL 8 DAY"); err != nil {
		t.Fatal(err)
	}
	if err := task.Exec(ctx); !errors.Is(err, fail) {
		t.Fatalf("exec: %v", err)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM oc3_scheduler_runs WHERE begin_at < NOW() - INTERVAL 7 DAY").Scan(&n); err != nil || n != 0 {
		t.Errorf("%d old runs left, %v", n, err)
	}
}
