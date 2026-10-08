package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// oidcMappingsFrom is the FROM clause of the claim rule lists: a derived table
// named after auth_oidc_mappings, where each rule carries the ids and the names of
// the teams it grants, in the order of the names, so that they can be selected,
// filtered and sorted like the other columns.
const oidcMappingsFrom = ` FROM (
	SELECT m.id, m.claim, m.value, m.allow_access, m.author, m.updated,
		COALESCE(GROUP_CONCAT(g.id ORDER BY g.role SEPARATOR ','), '') AS group_ids,
		COALESCE(GROUP_CONCAT(g.role ORDER BY g.role SEPARATOR ', '), '') AS group_roles
	FROM auth_oidc_mappings m
	LEFT JOIN auth_oidc_mapping_groups mg ON mg.mapping_id = m.id
	LEFT JOIN auth_group g ON g.id = mg.group_id
	GROUP BY m.id
) auth_oidc_mappings`

// GetOIDCMappings returns the rules translating OIDC claims into access and
// teams, with the column filters, sort and pagination of the request.
func (oDb *DB) GetOIDCMappings(ctx context.Context, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("GetOIDCMappings: no select expressions")
	}
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") + oidcMappingsFrom + " WHERE auth_oidc_mappings.id > 0"
	filterConds, args := p.FilterConditions()
	for _, cond := range filterConds {
		query += " AND " + cond
	}
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("auth_oidc_mappings.claim, auth_oidc_mappings.value, auth_oidc_mappings.id")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("GetOIDCMappings: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// GetOIDCMapping returns one rule.
func (oDb *DB) GetOIDCMapping(ctx context.Context, id string, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("GetOIDCMapping: no select expressions")
	}
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") + oidcMappingsFrom + " WHERE auth_oidc_mappings.id = ?"
	rows, err := oDb.DB.QueryContext(ctx, query, id)
	if err != nil {
		return nil, fmt.Errorf("GetOIDCMapping: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// OIDCMapping is a rule translating a claim value into access and teams.
type OIDCMapping struct {
	ID          int64
	Claim       string
	Value       string
	AllowAccess bool
	// GroupIDs are the teams granted, GroupRoles their names, in the same order.
	GroupIDs   []int64
	GroupRoles []string
}

// OIDCMappingByID returns one rule, nil when there is none with this id.
func (oDb *DB) OIDCMappingByID(ctx context.Context, id int64) (*OIDCMapping, error) {
	var (
		m      OIDCMapping
		access string
		ids    string
		roles  string
	)
	err := oDb.DB.QueryRowContext(ctx,
		"SELECT id, claim, value, allow_access, group_ids, group_roles"+oidcMappingsFrom+
			" WHERE auth_oidc_mappings.id = ?", id).
		Scan(&m.ID, &m.Claim, &m.Value, &access, &ids, &roles)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("OIDCMappingByID: %w", err)
	}
	m.AllowAccess = access == "T"
	if ids != "" {
		for _, s := range strings.Split(ids, ",") {
			if n, err := strconv.ParseInt(s, 10, 64); err == nil {
				m.GroupIDs = append(m.GroupIDs, n)
			}
		}
		m.GroupRoles = strings.Split(roles, ", ")
	}
	return &m, nil
}

// OIDCMappingWrite holds what is written when creating or changing a rule.
type OIDCMappingWrite struct {
	Claim       string
	Value       string
	AllowAccess bool
	// GroupIDs are the teams the rule grants, none when it only allows signing in.
	GroupIDs []int64
	Author   string
}

func tf(b bool) string {
	if b {
		return "T"
	}
	return "F"
}

// setOIDCMappingGroups replaces the teams a rule grants.
func (oDb *DB) setOIDCMappingGroups(ctx context.Context, id int64, groups []int64) error {
	if _, err := oDb.ExecContext(ctx, "DELETE FROM auth_oidc_mapping_groups WHERE mapping_id = ?", id); err != nil {
		return err
	}
	for _, g := range groups {
		if _, err := oDb.ExecContext(ctx,
			"INSERT INTO auth_oidc_mapping_groups (mapping_id, group_id) VALUES (?, ?)", id, g); err != nil {
			return err
		}
	}
	oDb.SetChange("auth_oidc_mapping_groups")
	return nil
}

// InsertOIDCMapping creates a rule and its teams, and returns its id. Meant to run
// in a transaction.
func (oDb *DB) InsertOIDCMapping(ctx context.Context, m OIDCMappingWrite) (int64, error) {
	res, err := oDb.ExecContext(ctx,
		`INSERT INTO auth_oidc_mappings (claim, value, allow_access, author, updated)
		VALUES (?, ?, ?, ?, NOW())`,
		m.Claim, m.Value, tf(m.AllowAccess), m.Author)
	if err != nil {
		return 0, fmt.Errorf("InsertOIDCMapping: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("InsertOIDCMapping lastInsertId: %w", err)
	}
	if err := oDb.setOIDCMappingGroups(ctx, id, m.GroupIDs); err != nil {
		return 0, fmt.Errorf("InsertOIDCMapping groups: %w", err)
	}
	oDb.SetChange("auth_oidc_mappings")
	return id, nil
}

// UpdateOIDCMapping replaces the definition of a rule and its teams. Meant to run
// in a transaction.
func (oDb *DB) UpdateOIDCMapping(ctx context.Context, id int64, m OIDCMappingWrite) error {
	if _, err := oDb.ExecContext(ctx,
		`UPDATE auth_oidc_mappings SET claim = ?, value = ?, allow_access = ?, author = ?, updated = NOW()
		WHERE id = ?`,
		m.Claim, m.Value, tf(m.AllowAccess), m.Author, id); err != nil {
		return fmt.Errorf("UpdateOIDCMapping: %w", err)
	}
	if err := oDb.setOIDCMappingGroups(ctx, id, m.GroupIDs); err != nil {
		return fmt.Errorf("UpdateOIDCMapping groups: %w", err)
	}
	oDb.SetChange("auth_oidc_mappings")
	return nil
}

// DeleteOIDCMapping deletes a rule and its teams. Meant to run in a transaction.
func (oDb *DB) DeleteOIDCMapping(ctx context.Context, id int64) error {
	if _, err := oDb.ExecContext(ctx, "DELETE FROM auth_oidc_mapping_groups WHERE mapping_id = ?", id); err != nil {
		return fmt.Errorf("DeleteOIDCMapping groups: %w", err)
	}
	if _, err := oDb.ExecContext(ctx, "DELETE FROM auth_oidc_mappings WHERE id = ?", id); err != nil {
		return fmt.Errorf("DeleteOIDCMapping: %w", err)
	}
	oDb.SetChange("auth_oidc_mappings", "auth_oidc_mapping_groups")
	return nil
}

// OIDCMappingDuplicate returns the id of another rule on the same claim and value,
// if any: one rule per claim value, holding every team it grants.
func (oDb *DB) OIDCMappingDuplicate(ctx context.Context, claim, value string, exceptID int64) (int64, bool, error) {
	var id int64
	err := oDb.DB.QueryRowContext(ctx,
		"SELECT id FROM auth_oidc_mappings WHERE claim = ? AND value = ? AND id <> ? LIMIT 1",
		claim, value, exceptID).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("OIDCMappingDuplicate: %w", err)
	}
	return id, true, nil
}

// GroupRoleByID returns the name of a team, and whether it exists.
func (oDb *DB) GroupRoleByID(ctx context.Context, id int64) (string, bool, error) {
	var role sql.NullString
	err := oDb.DB.QueryRowContext(ctx, "SELECT role FROM auth_group WHERE id = ?", id).Scan(&role)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("GroupRoleByID: %w", err)
	}
	return role.String, true, nil
}

// SyncMappedGroups aligns the memberships of the account userID to the teams in
// managed: it joins those in wanted and leaves the others. The teams the rules
// granted the account before, recorded in auth_oidc_memberships, are aligned too,
// so that one no rule names any more is left. Other teams are not touched. The
// memberships in wanted are recorded as granted by the rules. It returns the
// names of the teams joined and left.
func (oDb *DB) SyncMappedGroups(ctx context.Context, userID int64, managed, wanted []int64) (joined, left []string, err error) {
	granted, err := oDb.int64Column(ctx,
		"SELECT group_id FROM auth_oidc_memberships WHERE user_id = ?", userID)
	if err != nil {
		return nil, nil, fmt.Errorf("SyncMappedGroups: %w", err)
	}
	if len(managed) == 0 && len(granted) == 0 {
		return nil, nil, nil
	}
	want := make(map[int64]bool, len(wanted))
	for _, id := range wanted {
		want[id] = true
	}
	current, err := oDb.int64Column(ctx,
		"SELECT group_id FROM auth_membership WHERE user_id = ? AND group_id IS NOT NULL", userID)
	if err != nil {
		return nil, nil, fmt.Errorf("SyncMappedGroups: %w", err)
	}
	has := make(map[int64]bool, len(current))
	for _, id := range current {
		has[id] = true
	}
	seen := map[int64]bool{}
	for _, id := range append(append([]int64{}, managed...), granted...) {
		if seen[id] {
			continue
		}
		seen[id] = true
		switch {
		case want[id] && !has[id]:
			if _, err := oDb.ExecContext(ctx,
				"INSERT INTO auth_membership (user_id, group_id, primary_group) VALUES (?, ?, 'F')", userID, id); err != nil {
				return joined, left, fmt.Errorf("SyncMappedGroups join %d: %w", id, err)
			}
			role, _, _ := oDb.GroupRoleByID(ctx, id)
			joined = append(joined, role)
		case !want[id] && has[id]:
			if _, err := oDb.ExecContext(ctx,
				"DELETE FROM auth_membership WHERE user_id = ? AND group_id = ?", userID, id); err != nil {
				return joined, left, fmt.Errorf("SyncMappedGroups leave %d: %w", id, err)
			}
			role, _, _ := oDb.GroupRoleByID(ctx, id)
			left = append(left, role)
		}
	}
	if _, err := oDb.ExecContext(ctx, "DELETE FROM auth_oidc_memberships WHERE user_id = ?", userID); err != nil {
		return joined, left, fmt.Errorf("SyncMappedGroups record: %w", err)
	}
	for _, id := range wanted {
		if _, err := oDb.ExecContext(ctx,
			"INSERT IGNORE INTO auth_oidc_memberships (user_id, group_id) VALUES (?, ?)", userID, id); err != nil {
			return joined, left, fmt.Errorf("SyncMappedGroups record %d: %w", id, err)
		}
	}
	if len(joined) > 0 || len(left) > 0 {
		oDb.SetChange("auth_membership")
	}
	return joined, left, nil
}

// RevokedMembership is a membership the claim rules had granted, revoked once no
// rule names its team any more.
type RevokedMembership struct {
	Email string
	Role  string
}

// RevokeUnmappedGroups revokes the memberships the claim rules granted in the
// teams no rule names any more, at once rather than at the next sign-in of each
// account: a Bearer token, which reads the memberships of the account for the
// teams no rule names, would otherwise keep them.
func (oDb *DB) RevokeUnmappedGroups(ctx context.Context) ([]RevokedMembership, error) {
	const unmapped = "o.group_id NOT IN (SELECT group_id FROM auth_oidc_mapping_groups)"
	rows, err := oDb.DB.QueryContext(ctx,
		`SELECT o.user_id, o.group_id, COALESCE(u.email, ''), COALESCE(g.role, '')
		FROM auth_oidc_memberships o
		LEFT JOIN auth_user u ON u.id = o.user_id
		LEFT JOIN auth_group g ON g.id = o.group_id
		WHERE `+unmapped+` ORDER BY g.role, u.email`)
	if err != nil {
		return nil, fmt.Errorf("RevokeUnmappedGroups: %w", err)
	}
	type membership struct{ userID, groupID int64 }
	var memberships []membership
	var revoked []RevokedMembership
	for rows.Next() {
		var m membership
		var r RevokedMembership
		if err := rows.Scan(&m.userID, &m.groupID, &r.Email, &r.Role); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("RevokeUnmappedGroups scan: %w", err)
		}
		memberships = append(memberships, m)
		revoked = append(revoked, r)
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("RevokeUnmappedGroups: %w", err)
	}
	if len(memberships) == 0 {
		return nil, nil
	}
	for _, m := range memberships {
		if _, err := oDb.ExecContext(ctx,
			"DELETE FROM auth_membership WHERE user_id = ? AND group_id = ?", m.userID, m.groupID); err != nil {
			return nil, fmt.Errorf("RevokeUnmappedGroups leave: %w", err)
		}
	}
	if _, err := oDb.ExecContext(ctx, "DELETE o FROM auth_oidc_memberships o WHERE "+unmapped); err != nil {
		return nil, fmt.Errorf("RevokeUnmappedGroups record: %w", err)
	}
	oDb.SetChange("auth_membership")
	return revoked, nil
}

// int64Column returns the first column of the rows of query, NULLs left out.
func (oDb *DB) int64Column(ctx context.Context, query string, args ...any) ([]int64, error) {
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var ids []int64
	for rows.Next() {
		var id sql.NullInt64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		if id.Valid {
			ids = append(ids, id.Int64)
		}
	}
	return ids, rows.Err()
}
