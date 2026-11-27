-- 2026-11-24: the SMBIOS system uuid of the nodes, as the om3 agent reports it
-- in its asset (machine_uuid, /sys/class/dmi/id/product_uuid, lowercased).
--
-- A hypervisor sets it to the uuid of the virtual machine, which its
-- container.kvm or container.vbox resource reports in the resource info (the
-- uuid key of resinfo): the two tie a guest node to the hypervisor node and
-- the container resource running it, as the wake of a guest node needs. NULL
-- for a node reporting none, or an agent too old to report it.
--
-- It is not the uuid of auth_node, the authentication token of the node.
ALTER TABLE nodes
  ADD COLUMN IF NOT EXISTS machine_uuid CHAR(36) CHARACTER SET ascii COLLATE ascii_general_ci NULL DEFAULT NULL
    COMMENT 'SMBIOS system uuid, as the agent reports it',
  ADD INDEX IF NOT EXISTS idx_machine_uuid (machine_uuid);
