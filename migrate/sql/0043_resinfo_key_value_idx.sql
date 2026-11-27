-- 2026-11-24: the resource info looked up by value: the wake of a guest node
-- finds the container resource of its hypervisor by the uuid of the virtual
-- machine, the resinfo row with res_key 'uuid' and the machine uuid of the
-- node as res_value. Built in place, without locking the writes of the feed.
ALTER TABLE resinfo ADD INDEX IF NOT EXISTS k_key_value (res_key, res_value(64)), ALGORITHM=INPLACE, LOCK=NONE;
