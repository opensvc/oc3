-- 2026-11-10: the clusters and nodes the collector provisions. An operator
-- declares a cluster (its name, the app of its nodes, its heartbeats) and its
-- nodes, then issues a bootstrap token per node. A node being provisioned
-- trades its token for its collector credentials and the configuration of its
-- cluster: the first one creates the cluster (the seed), the others ask to join
-- it, a member of the cluster claiming their enrollment and enrolling them with
-- the join token they hand (kept in the cache, never here). The collector holds
-- no secret of the clusters: the nodes trust each other by the join of om3.

-- A cluster the collector provisions. heartbeats is a JSON array of the hb#<n>
-- sections of the cluster configuration, n their rank from 1. seed_node_id is
-- the node that creates the cluster, elected at the first bootstrap; set at the
-- declaration of a cluster reporting already, so that every node joins it.
CREATE TABLE IF NOT EXISTS cluster_declarations (
  cluster_id CHAR(36) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,
  cluster_name VARCHAR(128) NOT NULL,
  app VARCHAR(64) NOT NULL DEFAULT '',
  heartbeats TEXT NOT NULL,
  seed_node_id CHAR(36) CHARACTER SET ascii COLLATE ascii_general_ci NULL,
  created_by VARCHAR(255) NOT NULL DEFAULT '',
  created DATETIME NOT NULL,
  updated DATETIME NOT NULL,
  PRIMARY KEY (cluster_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- The bootstrap tokens of the nodes, by their sha256: a token is shown once,
-- used once (used set), before it expires. Issuing one deletes the unused ones
-- of the node. cluster_id is the cluster the token bootstraps the node into: a
-- joining node reports its former cluster until it joined, so its node row may
-- not name the declared one meanwhile; a used token entitles it to enroll.
CREATE TABLE IF NOT EXISTS node_bootstrap_tokens (
  id INT NOT NULL AUTO_INCREMENT,
  node_id CHAR(36) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,
  cluster_id CHAR(36) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,
  token_hash CHAR(64) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,
  expires DATETIME NOT NULL,
  used DATETIME NULL,
  created_by VARCHAR(255) NOT NULL DEFAULT '',
  created DATETIME NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_node_bootstrap_tokens_hash (token_hash),
  KEY k_node_id (node_id),
  KEY k_cluster_id (cluster_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- The requests of bootstrapped nodes to join their cluster: pending until a
-- member claims one, then done or failed as the member reports; expired when
-- nobody claimed it in time.
CREATE TABLE IF NOT EXISTS cluster_enrollments (
  id INT NOT NULL AUTO_INCREMENT,
  cluster_id CHAR(36) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,
  node_id CHAR(36) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,
  nodename VARCHAR(255) NOT NULL,
  node_addr VARCHAR(255) NOT NULL,
  state ENUM('pending','claimed','done','failed','expired') NOT NULL DEFAULT 'pending',
  claimed_by_node_id CHAR(36) CHARACTER SET ascii COLLATE ascii_general_ci NULL,
  claimed_by VARCHAR(255) NULL,
  message TEXT NOT NULL,
  created DATETIME NOT NULL,
  updated DATETIME NOT NULL,
  expires DATETIME NOT NULL,
  PRIMARY KEY (id),
  KEY k_cluster_state (cluster_id, state, id),
  KEY k_node_id (node_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
