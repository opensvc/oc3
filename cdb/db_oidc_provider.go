package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// OIDCClient is a cluster as a client of the collector's OpenID Connect
// provider: its cluster id is the client id.
type OIDCClient struct {
	ClusterID   string
	ClusterName string
}

// OIDCClientByID returns the cluster of the client id, nil when there is none.
func (oDb *DB) OIDCClientByID(ctx context.Context, clientID string) (*OIDCClient, error) {
	var c OIDCClient
	err := oDb.DB.QueryRowContext(ctx,
		"SELECT cluster_id, cluster_name FROM clusters WHERE cluster_id = ?", clientID).
		Scan(&c.ClusterID, &c.ClusterName)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("OIDCClientByID: %w", err)
	}
	return &c, nil
}

// OIDCClientHosts returns the host names and addresses the redirection URIs of
// the cluster may name: the names and fqdn of its nodes, their addresses, and
// the nodes its configuration lists.
func (oDb *DB) OIDCClientHosts(ctx context.Context, clusterID string) ([]string, error) {
	hosts := []string{}
	add := func(s string) {
		s = strings.TrimSpace(s)
		if s != "" {
			hosts = append(hosts, s)
		}
	}
	rows, err := oDb.DB.QueryContext(ctx, `SELECT COALESCE(n.nodename, ''), COALESCE(n.fqdn, '') FROM nodes n WHERE n.cluster_id = ?
		UNION SELECT COALESCE(ip.addr, ''), '' FROM node_ip ip JOIN nodes n ON n.node_id = ip.node_id
			WHERE n.cluster_id = ? AND ip.addr IS NOT NULL`, clusterID, clusterID)
	if err != nil {
		return nil, fmt.Errorf("OIDCClientHosts: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var a, b string
		if err := rows.Scan(&a, &b); err != nil {
			return nil, fmt.Errorf("OIDCClientHosts: %w", err)
		}
		add(a)
		add(b)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("OIDCClientHosts: %w", err)
	}
	var nodes sql.NullString
	err = oDb.DB.QueryRowContext(ctx,
		`SELECT REPLACE(REPLACE(REPLACE(JSON_EXTRACT(cluster_data, '$.data.cluster.config.nodes'), '[', ''), ']', ''), '"', '')
		FROM clusters WHERE cluster_id = ?`, clusterID).Scan(&nodes)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("OIDCClientHosts: %w", err)
	}
	for _, s := range strings.Split(nodes.String, ",") {
		add(s)
	}
	return hosts, nil
}

// OIDCUser is a collector account as the provider sees it.
type OIDCUser struct {
	ID     int64
	Email  string
	Name   string
	Locked bool
}

// OIDCUserByID returns the account, nil when there is none.
func (oDb *DB) OIDCUserByID(ctx context.Context, userID int64) (*OIDCUser, error) {
	var (
		u           OIDCUser
		first, last sql.NullString
		email, key  sql.NullString
	)
	err := oDb.DB.QueryRowContext(ctx,
		"SELECT id, first_name, last_name, email, registration_key FROM auth_user WHERE id = ?", userID).
		Scan(&u.ID, &first, &last, &email, &key)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("OIDCUserByID: %w", err)
	}
	u.Email = email.String
	u.Name = strings.TrimSpace(first.String + " " + last.String)
	u.Locked = strings.TrimSpace(key.String) != ""
	return &u, nil
}

// OIDCUserGrants returns the om3 grants lists of the rules naming a team of the
// user and the cluster, or every cluster.
func (oDb *DB) OIDCUserGrants(ctx context.Context, userID int64, clusterID string) ([]string, error) {
	rows, err := oDb.DB.QueryContext(ctx, `SELECT p.grants FROM oidc_provider_grants p
		JOIN auth_membership m ON m.group_id = p.group_id
		WHERE m.user_id = ? AND (p.cluster_id IS NULL OR p.cluster_id = ?)`, userID, clusterID)
	if err != nil {
		return nil, fmt.Errorf("OIDCUserGrants: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := []string{}
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, fmt.Errorf("OIDCUserGrants: %w", err)
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// OIDCGrantRule is a rule giving om3 grants to a team, on a cluster or on every
// cluster (ClusterID empty).
type OIDCGrantRule struct {
	ID          int64     `json:"id"`
	GroupID     int64     `json:"group_id"`
	GroupRole   string    `json:"group_role"`
	ClusterID   string    `json:"cluster_id"`
	ClusterName string    `json:"cluster_name"`
	Grants      string    `json:"grants"`
	Author      string    `json:"author"`
	Updated     time.Time `json:"updated"`
}

const oidcGrantRulesSelect = `SELECT p.id, p.group_id, COALESCE(g.role, ''), COALESCE(p.cluster_id, ''),
	COALESCE(c.cluster_name, ''), p.grants, p.author, p.updated
	FROM oidc_provider_grants p
	LEFT JOIN auth_group g ON g.id = p.group_id
	LEFT JOIN clusters c ON c.cluster_id = p.cluster_id`

func scanOIDCGrantRules(rows *sql.Rows) ([]OIDCGrantRule, error) {
	defer func() { _ = rows.Close() }()
	out := []OIDCGrantRule{}
	for rows.Next() {
		var r OIDCGrantRule
		if err := rows.Scan(&r.ID, &r.GroupID, &r.GroupRole, &r.ClusterID, &r.ClusterName, &r.Grants, &r.Author, &r.Updated); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// OIDCGrantRules returns the rules, the ones of every cluster first, then by
// cluster name and team.
func (oDb *DB) OIDCGrantRules(ctx context.Context) ([]OIDCGrantRule, error) {
	rows, err := oDb.DB.QueryContext(ctx, oidcGrantRulesSelect+
		" ORDER BY p.cluster_id IS NOT NULL, c.cluster_name, p.cluster_id, g.role, p.id")
	if err != nil {
		return nil, fmt.Errorf("OIDCGrantRules: %w", err)
	}
	out, err := scanOIDCGrantRules(rows)
	if err != nil {
		return nil, fmt.Errorf("OIDCGrantRules: %w", err)
	}
	return out, nil
}

// OIDCGrantRuleByID returns a rule, nil when there is none.
func (oDb *DB) OIDCGrantRuleByID(ctx context.Context, id int64) (*OIDCGrantRule, error) {
	rows, err := oDb.DB.QueryContext(ctx, oidcGrantRulesSelect+" WHERE p.id = ?", id)
	if err != nil {
		return nil, fmt.Errorf("OIDCGrantRuleByID: %w", err)
	}
	out, err := scanOIDCGrantRules(rows)
	if err != nil {
		return nil, fmt.Errorf("OIDCGrantRuleByID: %w", err)
	}
	if len(out) == 0 {
		return nil, nil
	}
	return &out[0], nil
}

// OIDCGrantRuleDuplicate returns the id of another rule of the same team and
// cluster, 0 when there is none.
func (oDb *DB) OIDCGrantRuleDuplicate(ctx context.Context, groupID int64, clusterID string, exceptID int64) (int64, error) {
	var id int64
	err := oDb.DB.QueryRowContext(ctx, `SELECT id FROM oidc_provider_grants
		WHERE group_id = ? AND COALESCE(cluster_id, '') = ? AND id <> ? LIMIT 1`, groupID, clusterID, exceptID).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("OIDCGrantRuleDuplicate: %w", err)
	}
	return id, nil
}

func nullableString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// InsertOIDCGrantRule adds a rule and returns its id.
func (oDb *DB) InsertOIDCGrantRule(ctx context.Context, groupID int64, clusterID, grants, author string) (int64, error) {
	res, err := oDb.ExecContext(ctx, `INSERT INTO oidc_provider_grants (group_id, cluster_id, grants, author, updated)
		VALUES (?, ?, ?, ?, NOW())`, groupID, nullableString(clusterID), grants, author)
	if err != nil {
		return 0, fmt.Errorf("InsertOIDCGrantRule: %w", err)
	}
	oDb.SetChange("oidc_provider_grants")
	return res.LastInsertId()
}

// UpdateOIDCGrantRule changes a rule.
func (oDb *DB) UpdateOIDCGrantRule(ctx context.Context, id, groupID int64, clusterID, grants, author string) error {
	if _, err := oDb.ExecContext(ctx, `UPDATE oidc_provider_grants
		SET group_id = ?, cluster_id = ?, grants = ?, author = ?, updated = NOW() WHERE id = ?`,
		groupID, nullableString(clusterID), grants, author, id); err != nil {
		return fmt.Errorf("UpdateOIDCGrantRule: %w", err)
	}
	oDb.SetChange("oidc_provider_grants")
	return nil
}

// DeleteOIDCGrantRule deletes a rule.
func (oDb *DB) DeleteOIDCGrantRule(ctx context.Context, id int64) error {
	if _, err := oDb.ExecContext(ctx, "DELETE FROM oidc_provider_grants WHERE id = ?", id); err != nil {
		return fmt.Errorf("DeleteOIDCGrantRule: %w", err)
	}
	oDb.SetChange("oidc_provider_grants")
	return nil
}

// OIDCRefreshToken is a refresh token as stored.
type OIDCRefreshToken struct {
	ID       int64
	Family   string
	UserID   int64
	ClientID string
	Scope    string
	AuthTime time.Time
	Expires  time.Time
	Used     bool
	Revoked  bool
}

// InsertOIDCRefreshToken stores a refresh token by its hash.
func (oDb *DB) InsertOIDCRefreshToken(ctx context.Context, hash string, t OIDCRefreshToken) error {
	if _, err := oDb.ExecContext(ctx, `INSERT INTO oidc_provider_refresh_tokens
		(token_hash, family, user_id, client_id, scope, auth_time, created, expires)
		VALUES (?, ?, ?, ?, ?, ?, NOW(), ?)`,
		hash, t.Family, t.UserID, t.ClientID, t.Scope, t.AuthTime.UTC(), t.Expires.UTC()); err != nil {
		return fmt.Errorf("InsertOIDCRefreshToken: %w", err)
	}
	return nil
}

// OIDCRefreshTokenByHash returns the token, nil when there is none.
func (oDb *DB) OIDCRefreshTokenByHash(ctx context.Context, hash string) (*OIDCRefreshToken, error) {
	var (
		t    OIDCRefreshToken
		used sql.NullTime
	)
	err := oDb.DB.QueryRowContext(ctx, `SELECT id, family, user_id, client_id, scope, auth_time, expires, used, revoked
		FROM oidc_provider_refresh_tokens WHERE token_hash = ?`, hash).
		Scan(&t.ID, &t.Family, &t.UserID, &t.ClientID, &t.Scope, &t.AuthTime, &t.Expires, &used, &t.Revoked)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("OIDCRefreshTokenByHash: %w", err)
	}
	t.Used = used.Valid
	return &t, nil
}

// UseOIDCRefreshToken marks the token used, and tells whether it was not
// already: of two requests with the same token, one only gets new tokens.
func (oDb *DB) UseOIDCRefreshToken(ctx context.Context, id int64) (bool, error) {
	n, err := oDb.ExecContextAndCountRowsAffected(ctx,
		"UPDATE oidc_provider_refresh_tokens SET used = NOW() WHERE id = ? AND used IS NULL AND revoked = 0", id)
	if err != nil {
		return false, fmt.Errorf("UseOIDCRefreshToken: %w", err)
	}
	return n == 1, nil
}

// RevokeOIDCRefreshFamily revokes the tokens of the family.
func (oDb *DB) RevokeOIDCRefreshFamily(ctx context.Context, family string) error {
	if _, err := oDb.ExecContext(ctx,
		"UPDATE oidc_provider_refresh_tokens SET revoked = 1 WHERE family = ?", family); err != nil {
		return fmt.Errorf("RevokeOIDCRefreshFamily: %w", err)
	}
	return nil
}

// RevokeOIDCRefreshTokens revokes the tokens of the user for the client.
func (oDb *DB) RevokeOIDCRefreshTokens(ctx context.Context, userID int64, clientID string) error {
	if _, err := oDb.ExecContext(ctx,
		"UPDATE oidc_provider_refresh_tokens SET revoked = 1 WHERE user_id = ? AND client_id = ? AND revoked = 0",
		userID, clientID); err != nil {
		return fmt.Errorf("RevokeOIDCRefreshTokens: %w", err)
	}
	return nil
}

// PurgeOIDCRefreshTokens deletes a batch of the tokens expired for a day.
func (oDb *DB) PurgeOIDCRefreshTokens(ctx context.Context) error {
	if _, err := oDb.ExecContext(ctx,
		"DELETE FROM oidc_provider_refresh_tokens WHERE expires < NOW() - INTERVAL 1 DAY LIMIT 500"); err != nil {
		return fmt.Errorf("PurgeOIDCRefreshTokens: %w", err)
	}
	return nil
}
