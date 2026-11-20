-- 2026-11-18: the scheduler tasks page.
--
-- The scheduler declares its tasks and their subtasks in oc3_scheduler when
-- it starts (a subtask is named "<task>: <subtask>", as in its logs), and
-- records each run: the last one on the task row, all of them in
-- oc3_scheduler_runs, kept 7 days.
ALTER TABLE oc3_scheduler
  ADD COLUMN IF NOT EXISTS parent varchar(256) NOT NULL DEFAULT ''
    COMMENT 'task running this subtask, empty for a task',
  ADD COLUMN IF NOT EXISTS task_desc varchar(512) NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS period_s int NOT NULL DEFAULT 0
    COMMENT 'seconds between two runs, 0: not scheduled',
  ADD COLUMN IF NOT EXISTS timeout_s int NOT NULL DEFAULT 0
    COMMENT 'seconds a run may last',
  ADD COLUMN IF NOT EXISTS declared tinyint(1) NOT NULL DEFAULT 0
    COMMENT 'declared by the scheduler at its last start',
  ADD COLUMN IF NOT EXISTS last_status varchar(16) NOT NULL DEFAULT ''
    COMMENT 'ok or failed',
  ADD COLUMN IF NOT EXISTS last_begin_at datetime(3) NULL DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS last_duration_ms int NULL DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS last_error text NULL DEFAULT NULL;

-- utf8mb3_general_ci as oc3_scheduler, whose task_name it is joined on.
CREATE TABLE IF NOT EXISTS oc3_scheduler_runs (
  id bigint unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY,
  task_name varchar(256) NOT NULL,
  begin_at datetime(3) NOT NULL,
  duration_ms int NOT NULL,
  status varchar(16) NOT NULL COMMENT 'ok or failed',
  error text NULL DEFAULT NULL,
  KEY k_task_begin (task_name, begin_at),
  KEY k_begin (begin_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
