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

-- 2026-10-07: the OpenID Connect identities of the users. A user signing in
-- through an OIDC provider is known by the pair (issuer, subject) the provider
-- gives, never by the email, which changes and can be reassigned. oc3 reads it at
-- every OIDC sign-in and for every Bearer access token, and writes it when an
-- identity is first linked to an auth_user account. A table of its own rather than
-- columns of auth_user, which the historical collector shares.
CREATE TABLE IF NOT EXISTS auth_user_identities (
  id         INT AUTO_INCREMENT PRIMARY KEY,
  user_id    INT NOT NULL,
  issuer     VARCHAR(255) NOT NULL,
  subject    VARCHAR(255) NOT NULL,
  created    DATETIME NOT NULL,
  last_login DATETIME NULL DEFAULT NULL,
  UNIQUE KEY uk_identity (issuer, subject),
  KEY k_user (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3;

-- 2026-10-07: the Everybody group, that every account belongs to. The historical
-- collector expects it (immutable group, user_add_evt trigger of auth_user adding
-- each new account to it), but never creates it: a database built from the
-- collector dump has none, and the trigger then leaves memberships with a NULL
-- group. Data rather than schema; idempotent. The memberships left with a NULL
-- group are given to Everybody, and an account without the membership gets it.
INSERT INTO auth_group (role, description, privilege)
  SELECT 'Everybody', 'Every user of the collector', 'F' FROM DUAL
  WHERE NOT EXISTS (SELECT 1 FROM auth_group WHERE role = 'Everybody');
UPDATE auth_membership
  SET group_id = (SELECT id FROM auth_group WHERE role = 'Everybody')
  WHERE group_id IS NULL;
INSERT IGNORE INTO auth_membership (user_id, group_id, primary_group)
  SELECT auth_user.id, (SELECT id FROM auth_group WHERE role = 'Everybody'), 'F'
  FROM auth_user
  WHERE NOT EXISTS (
    SELECT 1 FROM auth_membership m JOIN auth_group g ON g.id = m.group_id
    WHERE m.user_id = auth_user.id AND g.role = 'Everybody');
