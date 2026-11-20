package serverhandlers

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/migrate"
	"github.com/opensvc/oc3/server"
)

// schedulerTestAPI is an Api on a new database of the MariaDB server
// OC3_TEST_MYSQL_DSN names, holding oc3_scheduler as the collector created
// it migrated by 0039_scheduler_runs, and the log, dropped at the end of the
// test.
func schedulerTestAPI(t *testing.T) (*Api, *sql.DB) {
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
	name := fmt.Sprintf("oc3_test_schedtasks_%d", time.Now().UnixNano())
	if _, err := admin.Exec("CREATE DATABASE " + name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec("DROP DATABASE " + name) })
	cfg.DBName = name
	pool, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pool.Close() })
	statements := []string{
		"CREATE TABLE oc3_scheduler (id int(11) NOT NULL AUTO_INCREMENT, task_name varchar(256) DEFAULT NULL," +
			" is_disabled tinyint(1) DEFAULT 0, last_run_at datetime DEFAULT NULL, PRIMARY KEY (id), UNIQUE KEY uk (task_name))" +
			" DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci",
		"CREATE TABLE log (id INT AUTO_INCREMENT PRIMARY KEY, log_action VARCHAR(100) NOT NULL, log_user VARCHAR(100) NOT NULL," +
			" log_impersonator VARCHAR(100), log_origin VARCHAR(16), log_request_id VARCHAR(64), log_fmt VARCHAR(200), log_dict TEXT, log_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP," +
			" svc_id CHAR(36) DEFAULT '', log_level ENUM('debug','info','warning','error','critical') DEFAULT 'info', node_id CHAR(36) DEFAULT '')",
	}
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
		if _, err := pool.Exec(s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	initMetrics.Do(cdb.InitMetrics)
	odb := cdb.New(pool)
	odb.CreateSession(noEvents{})
	return &Api{ODB: odb, SyncTimeout: 10 * time.Second}, pool
}

// TestSchedulerTasks lists the tasks with their next run and the counts of
// their runs of the last 24 hours, lists the runs, and disables and enables a
// task: a Manager only, reading as changing, not a subtask nor an undeclared
// task, each change logged.
func TestSchedulerTasks(t *testing.T) {
	a, pool := schedulerTestAPI(t)
	testExec := func(statements ...string) {
		t.Helper()
		for _, s := range statements {
			if _, err := pool.Exec(s); err != nil {
				t.Fatalf("%s: %v", s, err)
			}
		}
	}
	testExec(
		"INSERT INTO oc3_scheduler (task_name, parent, period_s, timeout_s, declared, last_run_at, last_status) VALUES"+
			" ('alerts_1d', '', 86400, 600, 1, NOW() - INTERVAL 1 HOUR, 'failed'),"+
			" ('alerts_1d: alert_frozen', 'alerts_1d', 86400, 15, 1, NULL, 'ok'),"+
			" ('scrub_1m', '', 60, 60, 1, NOW(), 'ok'),"+
			" ('gone', '', 60, 60, 0, NOW(), '')",
		"INSERT INTO oc3_scheduler_runs (task_name, begin_at, duration_ms, status, error) VALUES"+
			" ('alerts_1d', NOW() - INTERVAL 1 HOUR, 9000, 'failed', 'boom'),"+
			" ('alerts_1d', NOW() - INTERVAL 2 DAY, 1000, 'ok', NULL),"+
			" ('scrub_1m', NOW() - INTERVAL 2 MINUTE, 100, 'ok', NULL),"+
			" ('scrub_1m', NOW() - INTERVAL 1 MINUTE, 300, 'ok', NULL)",
	)
	manager := []string{"Manager"}
	ops := []string{"Ops"}
	expect := func(rec *httptest.ResponseRecorder, code int, what string) {
		t.Helper()
		if rec.Code != code {
			t.Fatalf("%s: %d %s, want %d", what, rec.Code, rec.Body.String(), code)
		}
	}

	c, rec := aiContext(http.MethodGet, "/scheduler/tasks", "", ops...)
	_ = a.GetSchedulerTasks(c, server.GetSchedulerTasksParams{})
	expect(rec, http.StatusForbidden, "list as Ops")
	c, rec = aiContext(http.MethodGet, "/scheduler/runs", "", ops...)
	_ = a.GetSchedulerRuns(c, server.GetSchedulerRunsParams{})
	expect(rec, http.StatusForbidden, "list the runs as Ops")
	c, rec = aiContext(http.MethodGet, "/scheduler/tasks/alerts_1d", "", ops...)
	_ = a.GetSchedulerTask(c, "alerts_1d", server.GetSchedulerTaskParams{})
	expect(rec, http.StatusForbidden, "read a task as Ops")

	c, rec = aiContext(http.MethodGet, "/scheduler/tasks", "", manager...)
	_ = a.GetSchedulerTasks(c, server.GetSchedulerTasksParams{})
	expect(rec, http.StatusOK, "list as Manager")
	rows := sysrepRows(t, rec)
	var names []string
	for _, r := range rows {
		names = append(names, r["task_name"].(string))
	}
	if fmt.Sprint(names) != "[alerts_1d alerts_1d: alert_frozen gone scrub_1m]" {
		t.Fatalf("order %v: a subtask follows its task", names)
	}
	alerts := rows[0]
	if alerts["runs_24h"] != float64(1) || alerts["failures_24h"] != float64(1) || alerts["avg_duration_ms_24h"] != float64(9000) ||
		alerts["next_run_at"] == "" || alerts["is_disabled"] != false {
		t.Errorf("alerts_1d %v", alerts)
	}
	if sub := rows[1]; sub["next_run_at"] != "" || sub["runs_24h"] != float64(0) || sub["avg_duration_ms_24h"] != nil {
		t.Errorf("subtask %v", sub)
	}
	if scrub := rows[3]; scrub["runs_24h"] != float64(2) || scrub["avg_duration_ms_24h"] != float64(200) || scrub["max_duration_ms_24h"] != float64(300) {
		t.Errorf("scrub_1m %v", scrub)
	}

	c, rec = aiContext(http.MethodGet, "/scheduler/tasks", "")
	c.Set(XAuthMode, AuthModeNode)
	_ = a.GetSchedulerTasks(c, server.GetSchedulerTasksParams{})
	expect(rec, http.StatusUnauthorized, "list as a node")

	filter := server.InQueryFilter{"status:eq:failed"}
	c, rec = aiContext(http.MethodGet, "/scheduler/runs", "", manager...)
	_ = a.GetSchedulerRuns(c, server.GetSchedulerRunsParams{Filter: &filter})
	expect(rec, http.StatusOK, "list the failed runs")
	if runs := sysrepRows(t, rec); len(runs) != 1 || runs[0]["error"] != "boom" || runs[0]["task_name"] != "alerts_1d" {
		t.Errorf("failed runs %v", runs)
	}
	c, rec = aiContext(http.MethodGet, "/scheduler/runs", "", manager...)
	_ = a.GetSchedulerRuns(c, server.GetSchedulerRunsParams{})
	if runs := sysrepRows(t, rec); len(runs) != 4 || runs[0]["task_name"] != "scrub_1m" || runs[0]["duration_ms"] != float64(300) {
		t.Errorf("runs, the last first: %v", runs)
	}

	put := func(groups []string, task, body string) *httptest.ResponseRecorder {
		c, rec := aiContext(http.MethodPut, "/scheduler/tasks/"+url.PathEscape(task), body, groups...)
		_ = a.PutSchedulerTask(c, task)
		return rec
	}
	expect(put(ops, "alerts_1d", `{"is_disabled":true}`), http.StatusForbidden, "disable as Ops")
	expect(put(manager, "alerts_1d: alert_frozen", `{"is_disabled":true}`), http.StatusBadRequest, "disable a subtask")
	expect(put(manager, "gone", `{"is_disabled":true}`), http.StatusNotFound, "disable an undeclared task")
	expect(put(manager, "nope", `{"is_disabled":true}`), http.StatusNotFound, "disable an unknown task")
	rec = put(manager, "alerts_1d", `{"is_disabled":true}`)
	expect(rec, http.StatusOK, "disable")
	if r := sysrepRows(t, rec); len(r) != 1 || r[0]["is_disabled"] != true || r[0]["next_run_at"] != "" {
		t.Errorf("disabled %v", r)
	}
	expect(put(manager, "alerts_1d", `{"is_disabled":false}`), http.StatusOK, "enable")
	var logs int
	if err := pool.QueryRow("SELECT COUNT(*) FROM log WHERE log_action IN ('scheduler.task.disable', 'scheduler.task.enable')").Scan(&logs); err != nil || logs != 2 {
		t.Errorf("%d log entries, %v", logs, err)
	}

	c, rec = aiContext(http.MethodGet, "/scheduler/tasks/x", "", manager...)
	_ = a.GetSchedulerTask(c, "alerts_1d: alert_frozen", server.GetSchedulerTaskParams{})
	expect(rec, http.StatusOK, "read a subtask")
}
