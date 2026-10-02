package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// GetUserGroups lists the groups a user visible to the caller is member of, as
// rest_get_user_groups (init/models/rest/api_users.py:288) does: the user is
// designated as GetUser designates them, and is visible by the same rule.
func (oDb *DB) GetUserGroups(ctx context.Context, idOrEmail string, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getUserGroups: no select expressions")
	}
	identClause, identArgs, ok := userIdentClause(idOrEmail, p.UserID)
	if !ok {
		return nil, nil
	}
	authClause, args := usersAuthClause(p)
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") +
		" FROM auth_group" +
		" JOIN auth_membership ON auth_membership.group_id = auth_group.id" +
		" JOIN auth_user ON auth_user.id = auth_membership.user_id" +
		" WHERE " + authClause + " AND " + identClause
	args = append(args, identArgs...)
	filterConds, filterArgs := p.FilterConditions()
	for _, cond := range filterConds {
		query += " AND " + cond
	}
	args = append(args, filterArgs...)
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("auth_group.role")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getUserGroups: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// MembershipUser is the user a membership change is about.
type MembershipUser struct {
	ID    int64
	Email string
}

// MembershipUserByIdent returns the user an auth_user.id or an email designates,
// or nil.
func (oDb *DB) MembershipUserByIdent(ctx context.Context, idOrEmail string) (*MembershipUser, error) {
	query := "SELECT id, email FROM auth_user WHERE email = ?"
	args := []any{idOrEmail}
	if id, err := strconv.ParseInt(idOrEmail, 10, 64); err == nil {
		query = "SELECT id, email FROM auth_user WHERE id = ?"
		args = []any{id}
	}
	var u MembershipUser
	var email sql.NullString
	err := oDb.DB.QueryRowContext(ctx, query, args...).Scan(&u.ID, &email)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("membershipUserByIdent: %w", err)
	}
	u.Email = email.String
	return &u, nil
}

// MembershipGroupByIdent returns the group an auth_group.id or a role
// designates, or nil. A caller who is not a Manager can only designate one of
// their own groups, as rest_post_user_group restricts it.
func (oDb *DB) MembershipGroupByIdent(ctx context.Context, idOrRole string, callerGroups []string, isManager bool) (*AuthGroup, error) {
	query := "SELECT id, role, privilege, COALESCE(description, '') FROM auth_group WHERE "
	args := []any{}
	if id, err := strconv.ParseInt(idOrRole, 10, 64); err == nil {
		query += "id = ?"
		args = append(args, id)
	} else {
		query += "role = ?"
		args = append(args, idOrRole)
	}
	if !isManager {
		cleanG := cleanGroups(callerGroups)
		if len(cleanG) == 0 {
			return nil, nil
		}
		query += " AND role IN (" + Placeholders(len(cleanG)) + ")"
		args = append(args, stringsToAny(cleanG)...)
	}
	var g AuthGroup
	var privilege sql.NullString
	err := oDb.DB.QueryRowContext(ctx, query, args...).Scan(&g.ID, &g.Role, &privilege, &g.Description)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("membershipGroupByIdent: %w", err)
	}
	g.Privilege = privilege.String == "T"
	return &g, nil
}

// MembershipExists tells whether the user is member of the group.
func (oDb *DB) MembershipExists(ctx context.Context, userID, groupID int64) (bool, error) {
	var n int
	err := oDb.DB.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM auth_membership WHERE user_id = ? AND group_id = ?", userID, groupID).Scan(&n)
	if err != nil {
		return false, fmt.Errorf("membershipExists: %w", err)
	}
	return n > 0, nil
}

// DeleteGroupMembership removes a user from a group.
func (oDb *DB) DeleteGroupMembership(ctx context.Context, userID, groupID int64) error {
	if _, err := oDb.DB.ExecContext(ctx,
		"DELETE FROM auth_membership WHERE user_id = ? AND group_id = ?", userID, groupID); err != nil {
		return fmt.Errorf("deleteGroupMembership: %w", err)
	}
	oDb.SetChange("auth_membership")
	return nil
}
