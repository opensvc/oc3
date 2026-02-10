package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// return the default app for a user
// if the user belongs to several apps, return the first one in alphabetical order
func (oDb *DB) UserDefaultApp(ctx context.Context, userID *int64) (string, error) {
	if userID == nil {
		return "", fmt.Errorf("userDefaultApp: missing user id")
	}

	groupIDs, err := oDb.userGroupIDs(ctx, userID)
	if err != nil {
		return "", fmt.Errorf("userDefaultApp: %w", err)
	}
	if len(groupIDs) == 0 {
		return "", nil
	}

	groupClause, args := inClause("apps_responsibles.group_id", toAnyInt64Slice(groupIDs))
	query := "SELECT apps.app FROM apps " +
		"JOIN apps_responsibles ON apps_responsibles.app_id = apps.id " +
		"WHERE " + groupClause + " AND apps.app <> '' AND apps.app IS NOT NULL " +
		"ORDER BY apps.app LIMIT 1"

	var app sql.NullString

	err = oDb.DB.QueryRowContext(ctx, query, args...).Scan(&app)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "", nil
	case err != nil:
		return "", fmt.Errorf("userDefaultApp: %w", err)
	case !app.Valid || app.String == "":
		return "", nil
	default:
		return app.String, nil
	}
}

// return the list of group IDs a user belongs to.
func (oDb *DB) userGroupIDs(ctx context.Context, userID *int64) ([]int64, error) {
	if userID == nil {
		return nil, fmt.Errorf("userGroupIDs: missing user id")
	}

	const query = "SELECT auth_group.id FROM auth_group " +
		"JOIN auth_membership ON auth_membership.group_id = auth_group.id " +
		"WHERE auth_membership.user_id = ?"
	rows, err := oDb.DB.QueryContext(ctx, query, *userID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	groupIDs := []int64{}
	for rows.Next() {
		var groupID int64
		if err := rows.Scan(&groupID); err != nil {
			return nil, err
		}
		groupIDs = append(groupIDs, groupID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return groupIDs, nil
}

// return the primary group ID for a user
func (oDb *DB) UserPrimaryGroupID(ctx context.Context, userID int64) (int64, bool, error) {
	const query = "SELECT auth_group.id FROM auth_group " +
		"JOIN auth_membership ON auth_membership.group_id = auth_group.id " +
		"WHERE auth_membership.user_id = ? AND auth_membership.primary_group = 'T' " +
		"LIMIT 1"

	var groupID sql.NullInt64
	err := oDb.DB.QueryRowContext(ctx, query, userID).Scan(&groupID)
	return checkRow(err, groupID.Valid, groupID.Int64)
}

// return the private group ID for a user (group with role 'user_<userID>' and privilege 'F')
func (oDb *DB) UserPrivateGroupID(ctx context.Context, userID int64) (int64, bool, error) {
	const query = "SELECT auth_group.id FROM auth_group " +
		"JOIN auth_membership ON auth_membership.group_id = auth_group.id " +
		"WHERE auth_membership.user_id = ? AND auth_group.role LIKE 'user_%' AND auth_group.privilege = 'F' " +
		"LIMIT 1"
	var groupID sql.NullInt64
	err := oDb.DB.QueryRowContext(ctx, query, userID).Scan(&groupID)
	return checkRow(err, groupID.Valid, groupID.Int64)
}

// / return the default group ID for a user
// - first try to find a primary group
// - if not found, try to find a private group
// - if not found, try to find a group with privilege 'F' and role != 'Everybody'
func (oDb *DB) UserDefaultGroupID(ctx context.Context, userID int64) (int64, bool, error) {
	if gid, ok, err := oDb.UserPrimaryGroupID(ctx, userID); err != nil {
		return 0, false, err
	} else if ok {
		return gid, true, nil
	}

	if gid, ok, err := oDb.UserPrivateGroupID(ctx, userID); err != nil {
		return 0, false, err
	} else if ok {
		return gid, true, nil
	}

	const query = "SELECT auth_group.id FROM auth_group " +
		"JOIN auth_membership ON auth_membership.group_id = auth_group.id " +
		"WHERE auth_membership.user_id = ? AND auth_group.privilege = 'F' AND auth_group.role != 'Everybody' " +
		"ORDER BY auth_group.role LIMIT 1"

	var groupID sql.NullInt64
	err := oDb.DB.QueryRowContext(ctx, query, userID).Scan(&groupID)
	return checkRow(err, groupID.Valid, groupID.Int64)
}

// return the default group for a user
func (oDb *DB) UserDefaultGroup(ctx context.Context, userID int64) (string, bool, error) {
	gid, ok, err := oDb.UserDefaultGroupID(ctx, userID)
	if err != nil {
		return "", false, err
	}
	if !ok {
		return "", false, nil
	}

	const query = "SELECT role FROM auth_group WHERE id = ?"
	var role sql.NullString
	err = oDb.DB.QueryRowContext(ctx, query, gid).Scan(&role)
	return checkRow(err, role.Valid, role.String)
}
