-- 2026-11-19: the clusters the nodes claim, not confirmed yet. A node reports
-- its cluster id (daemon status, registration): the collector trusts it only
-- for a cluster it does not know, or once a confirmed member of the cluster
-- lists the node in its own daemon status. Meanwhile the claim waits here and
-- nodes.cluster_id keeps the confirmed cluster, the one the rights rest on
-- (reports for the peers, enrollments, compliance).
CREATE TABLE IF NOT EXISTS node_cluster_claims (
  node_id CHAR(36) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,
  cluster_id CHAR(36) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,
  created DATETIME NOT NULL,
  updated DATETIME NOT NULL,
  PRIMARY KEY (node_id),
  KEY idx_node_cluster_claims_cluster_id (cluster_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
