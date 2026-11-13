-- 2026-11-12: the collector as the OpenID Connect provider of the om3 clusters.
--
-- Each cluster the collector knows is a client of the provider, its cluster id
-- the client id: the tokens a user gets for a cluster name it as their audience,
-- and carry the om3 grants (entitlements claim) the user holds on it.

-- The om3 grants a team gives on a cluster, or on every cluster when cluster_id
-- is NULL. grants is the space separated list of om3 grants (root,
-- admin:<namespace>, guest:*...). The grants of a user on a cluster are those of
-- the rules naming one of their teams and this cluster or every cluster.
CREATE TABLE IF NOT EXISTS oidc_provider_grants (
  id INT NOT NULL AUTO_INCREMENT,
  group_id INT NOT NULL,
  cluster_id CHAR(36) CHARACTER SET ascii COLLATE ascii_general_ci NULL,
  grants VARCHAR(1024) NOT NULL,
  author VARCHAR(255) NOT NULL DEFAULT '',
  updated DATETIME NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY idx_group_cluster (group_id, cluster_id),
  KEY idx_cluster (cluster_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- The refresh tokens handed to the clients, by their sha256. A refresh token is
-- used once: the token endpoint hands a new one of the same family. A token
-- used again tells it leaked: the whole family is revoked. expires is the end
-- of the idle lifetime, auth_time the sign-in the family comes from, which
-- bounds its whole lifetime.
CREATE TABLE IF NOT EXISTS oidc_provider_refresh_tokens (
  id BIGINT NOT NULL AUTO_INCREMENT,
  token_hash CHAR(64) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,
  family CHAR(32) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,
  user_id INT NOT NULL,
  client_id CHAR(36) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,
  scope VARCHAR(512) NOT NULL DEFAULT '',
  auth_time DATETIME NOT NULL,
  created DATETIME NOT NULL,
  expires DATETIME NOT NULL,
  used DATETIME NULL,
  revoked TINYINT(1) NOT NULL DEFAULT 0,
  PRIMARY KEY (id),
  UNIQUE KEY idx_token_hash (token_hash),
  KEY idx_family (family),
  KEY idx_user_client (user_id, client_id),
  KEY idx_expires (expires)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- The Manager team is root on every cluster, as it is the collector's. The
-- unique key does not hold the NULL cluster_id rows (NULLs differ): the rule is
-- added only when the team has none for every cluster, so that the migration
-- may run again.
INSERT INTO oidc_provider_grants (group_id, cluster_id, grants, author, updated)
  SELECT g.id, NULL, 'root', 'migration', NOW() FROM auth_group g
  WHERE g.role = 'Manager'
    AND NOT EXISTS (SELECT 1 FROM oidc_provider_grants p WHERE p.group_id = g.id AND p.cluster_id IS NULL);
