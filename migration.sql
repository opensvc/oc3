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
