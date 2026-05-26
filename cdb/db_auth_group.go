package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
)

type OrgGroupErrCode int

const (
	OrgGroupOK OrgGroupErrCode = iota
	OrgGroupNotFound
	OrgGroupAmbiguous
	OrgGroupPrivileged
)

func (oDb *DB) OrgGroup(ctx context.Context, idOrRole string, userGroupIDs []int64, isManager bool) (*AuthGroup, OrgGroupErrCode, error) {
	if idOrRole == "" {
		return nil, OrgGroupNotFound, nil
	}

	query := "SELECT id, role, privilege, COALESCE(description, '') FROM auth_group WHERE "
	args := []any{}

	if id, err := strconv.ParseInt(idOrRole, 10, 64); err == nil {
		query += "(id = ? OR role = ?)"
		args = append(args, id, idOrRole)
	} else {
		query += "role = ?"
		args = append(args, idOrRole)
	}

	if !isManager {
		if len(userGroupIDs) == 0 {
			return nil, OrgGroupNotFound, nil
		}
		clause, inArgs := inClause("id", toAnyInt64Slice(userGroupIDs))
		query += " AND " + clause
		args = append(args, inArgs...)
	}

	query += " LIMIT 2"

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, OrgGroupOK, fmt.Errorf("OrgGroup: %w", err)
	}
	defer func() { _ = rows.Close() }()

	groups := make([]AuthGroup, 0, 2)
	for rows.Next() {
		var (
			g         AuthGroup
			role      sql.NullString
			privilege sql.NullString
		)
		if err := rows.Scan(&g.ID, &role, &privilege, &g.Description); err != nil {
			return nil, OrgGroupOK, fmt.Errorf("OrgGroup scan: %w", err)
		}
		if role.Valid {
			g.Role = role.String
		}
		if privilege.Valid {
			g.Privilege = privilege.String == "T"
		}
		groups = append(groups, g)
	}
	if err := rows.Err(); err != nil {
		return nil, OrgGroupOK, fmt.Errorf("OrgGroup rows: %w", err)
	}

	switch len(groups) {
	case 0:
		return nil, OrgGroupNotFound, nil
	case 1:
		if groups[0].Privilege {
			return &groups[0], OrgGroupPrivileged, nil
		}
		return &groups[0], OrgGroupOK, nil
	default:
		return nil, OrgGroupAmbiguous, nil
	}
}

func (oDb *DB) AuthGroupByIDOrRole(ctx context.Context, idOrRole string) (*AuthGroup, bool, error) {
	if idOrRole == "" {
		return nil, false, nil
	}
	query := "SELECT id, role, privilege, COALESCE(description, '') FROM auth_group WHERE "
	args := []any{}
	if id, err := strconv.ParseInt(idOrRole, 10, 64); err == nil {
		query += "(id = ? OR role = ?)"
		args = append(args, id, idOrRole)
	} else {
		query += "role = ?"
		args = append(args, idOrRole)
	}
	query += " LIMIT 2"

	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, false, fmt.Errorf("AuthGroupByIDOrRole: %w", err)
	}
	defer func() { _ = rows.Close() }()

	groups := make([]AuthGroup, 0, 2)
	for rows.Next() {
		var (
			g         AuthGroup
			role      sql.NullString
			privilege sql.NullString
		)
		if err := rows.Scan(&g.ID, &role, &privilege, &g.Description); err != nil {
			return nil, false, fmt.Errorf("AuthGroupByIDOrRole scan: %w", err)
		}
		if role.Valid {
			g.Role = role.String
		}
		if privilege.Valid {
			g.Privilege = privilege.String == "T"
		}
		groups = append(groups, g)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("AuthGroupByIDOrRole rows: %w", err)
	}

	if len(groups) != 1 {
		return nil, false, nil
	}
	return &groups[0], true, nil
}

// UserGroupIDs returns the list of group ids the user belongs to.
func (oDb *DB) UserGroupIDs(ctx context.Context, userID int64) ([]int64, error) {
	const query = "SELECT auth_group.id FROM auth_group " +
		"JOIN auth_membership ON auth_membership.group_id = auth_group.id " +
		"WHERE auth_membership.user_id = ?"
	rows, err := oDb.DB.QueryContext(ctx, query, userID)
	if err != nil {
		return nil, fmt.Errorf("UserGroupIDs: %w", err)
	}
	defer func() { _ = rows.Close() }()
	ids := []int64{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("UserGroupIDs: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("UserGroupIDs: %w", err)
	}
	return ids, nil
}

// AppPublicationExists reports whether (app_id, group_id) is present in apps_publications.
func (oDb *DB) AppPublicationExists(ctx context.Context, appID, groupID int64) (bool, error) {
	const query = "SELECT 1 FROM apps_publications WHERE app_id = ? AND group_id = ? LIMIT 1"
	var x int
	err := oDb.DB.QueryRowContext(ctx, query, appID, groupID).Scan(&x)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("AppPublicationExists: %w", err)
	default:
		return true, nil
	}
}

func (oDb *DB) AppResponsibleExists(ctx context.Context, appID, groupID int64) (bool, error) {
	const query = "SELECT 1 FROM apps_responsibles WHERE app_id = ? AND group_id = ? LIMIT 1"
	var x int
	err := oDb.DB.QueryRowContext(ctx, query, appID, groupID).Scan(&x)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("AppResponsibleExists: %w", err)
	default:
		return true, nil
	}
}

// removes App without publication from dashboard
func (oDb *DB) DeleteDashboardAppWithoutPublication(ctx context.Context, app string) error {
	const query = `DELETE FROM dashboard
		WHERE dash_type = 'application code without publication'
		  AND dash_dict LIKE ?`
	pattern := "%:\"" + app + "\"%"
	if _, err := oDb.DB.ExecContext(ctx, query, pattern); err != nil {
		return fmt.Errorf("DeleteDashboardAppWithoutPublication: %w", err)
	}
	oDb.SetChange("dashboard")
	return nil
}

// removes App without responsible from dashboard
func (oDb *DB) DeleteDashboardAppWithoutResponsible(ctx context.Context, app string) error {
	const query = `DELETE FROM dashboard
		WHERE dash_type = 'application code without responsible'
		  AND dash_dict LIKE ?`
	pattern := "%:\"" + app + "\"%"
	if _, err := oDb.DB.ExecContext(ctx, query, pattern); err != nil {
		return fmt.Errorf("DeleteDashboardAppWithoutResponsible: %w", err)
	}
	oDb.SetChange("dashboard")
	return nil
}
