package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
)

// userOwnedTables hold rows that belong to one account alone, removed with it.
// History stays: log, action_queue, alerts_sent, auth_event, form_output_results.
var userOwnedTables = []string{
	"auth_membership",
	"auth_user_identities",
	"auth_oidc_memberships",
	"gen_filterset_user",
	"user_prefs",
	"user_log",
	"reports_user",
	"stats_compare_user",
}

// DeleteUserCascade deletes an account and what belongs to it alone: the rows of
// userOwnedTables and its private team user_<id>, with what references the team.
// Meant to run in a transaction.
func (oDb *DB) DeleteUserCascade(ctx context.Context, userID int64) error {
	var groupID sql.NullInt64
	err := oDb.DB.QueryRowContext(ctx,
		"SELECT id FROM auth_group WHERE role = ?", "user_"+strconv.FormatInt(userID, 10)).Scan(&groupID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("DeleteUserCascade private group: %w", err)
	}
	if groupID.Valid {
		if err := oDb.DeleteGroupCascade(ctx, groupID.Int64); err != nil {
			return fmt.Errorf("DeleteUserCascade private group: %w", err)
		}
	}
	for _, t := range userOwnedTables {
		if _, err := oDb.ExecContext(ctx, "DELETE FROM "+t+" WHERE user_id = ?", userID); err != nil {
			return fmt.Errorf("DeleteUserCascade %s: %w", t, err)
		}
		oDb.SetChange(t)
	}
	if _, err := oDb.ExecContext(ctx, "DELETE FROM auth_user WHERE id = ?", userID); err != nil {
		return fmt.Errorf("DeleteUserCascade auth_user: %w", err)
	}
	oDb.SetChange("auth_user")
	return nil
}

// UserInRole tells whether the account userID is a member of the team role.
func (oDb *DB) UserInRole(ctx context.Context, userID int64, role string) (bool, error) {
	var n int
	err := oDb.DB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM auth_membership
		JOIN auth_group ON auth_group.id = auth_membership.group_id
		WHERE auth_membership.user_id = ? AND auth_group.role = ?`, userID, role).Scan(&n)
	if err != nil {
		return false, fmt.Errorf("UserInRole: %w", err)
	}
	return n > 0, nil
}

// RoleMemberCount returns how many accounts are members of the team role.
func (oDb *DB) RoleMemberCount(ctx context.Context, role string) (int, error) {
	var n int
	err := oDb.DB.QueryRowContext(ctx,
		`SELECT COUNT(DISTINCT auth_membership.user_id) FROM auth_membership
		JOIN auth_group ON auth_group.id = auth_membership.group_id
		JOIN auth_user ON auth_user.id = auth_membership.user_id
		WHERE auth_group.role = ?`, role).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("RoleMemberCount: %w", err)
	}
	return n, nil
}
