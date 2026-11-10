-- 2026-11-10: the services status update used to be an upsert. A service
-- deleted while a daemon status job held it was inserted again with its status
-- columns only, svcname NULL and cluster_id empty. The feed of its cluster then
-- failed at each run on scanning it ("converting NULL to string is
-- unsupported"). The status update no longer inserts and ObjectCreate repairs
-- such a row (876fbc9). The rows left go: the next daemon status of a service
-- still there creates it again with the same svc_id (from service_ids), the
-- others were deleted already. The column then refuses a row without a name.
-- Nothing left to delete when run again.
DELETE FROM services WHERE svcname IS NULL OR svcname = '';

ALTER TABLE services MODIFY svcname varchar(256) NOT NULL;
