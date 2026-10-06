-- Schema changes required by oc3, oldest first. Apply them in order to a
-- collector database; each statement says what needs it.

-- 2026-10-02: the availability target of a service, in percent, which the
-- Services view compares with its availability rate. NULL: no SLA.
ALTER TABLE services
  ADD COLUMN svc_sla DECIMAL(6,3) NULL DEFAULT NULL
  COMMENT 'availability target, in percent';

-- 2026-10-02: the availability rate of a service over the last 30 days, in
-- percent, stored so that the Services view can sort and filter by it. The
-- scheduler refreshes it every 10 minutes (task services_availability), and a
-- justification change at once. NULL: no status recorded.
ALTER TABLE services
  ADD COLUMN svc_availability DECIMAL(9,6) NULL DEFAULT NULL
    COMMENT 'availability rate of the last 30 days, in percent',
  ADD COLUMN svc_availability_updated DATETIME NULL DEFAULT NULL
    COMMENT 'when svc_availability was computed',
  ADD KEY k_svc_availability (svc_availability);

-- 2026-10-06: the user who really signed in when an action is made as another
-- user (impersonation). log_user names the user the action is made as; NULL here
-- when nobody is impersonated. Written by oc3 on every audit entry of an
-- impersonated request.
ALTER TABLE log
  ADD COLUMN log_impersonator VARCHAR(100) NULL DEFAULT NULL
    COMMENT 'user who really signed in, when acting as log_user'
    AFTER log_user;

-- 2026-10-06: the kind of an svcactions row, as the historical collector declares
-- it (init/models/db.py): 'status' for an action, begun or ended, '' for a log line
-- of an action. oc3 writes it with every action, and the actions of a service
-- (GET /services/{svc_id}/actions) are read by it; a database created before the
-- column existed refuses every action the agents report ("Unknown column
-- 'log_type'"). IF NOT EXISTS: the column is already there in a collector
-- database kept up to date by web2py.
ALTER TABLE svcactions
  ADD COLUMN IF NOT EXISTS log_type VARCHAR(30) DEFAULT '';
