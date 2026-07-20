package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

func (oDb *DB) GetGroups(ctx context.Context, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getGroups: no select expressions")
	}
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") + " FROM auth_group WHERE "
	args := []any{}
	if p.IsManager {
		query += "auth_group.id > 0"
	} else {
		cleanG := cleanGroups(p.Groups)
		if len(cleanG) == 0 {
			query += "1=0"
		} else {
			query += "auth_group.role IN (" + Placeholders(len(cleanG)) + ")"
			args = append(args, stringsToAny(cleanG)...)
		}
	}
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("auth_group.role, auth_group.id")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getGroups: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

func (oDb *DB) GetGroup(ctx context.Context, idOrRole string, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getGroup: no select expressions")
	}
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") + " FROM auth_group WHERE "
	args := []any{}
	if p.IsManager {
		query += "auth_group.id > 0"
	} else {
		cleanG := cleanGroups(p.Groups)
		if len(cleanG) == 0 {
			query += "1=0"
		} else {
			query += "auth_group.role IN (" + Placeholders(len(cleanG)) + ")"
			args = append(args, stringsToAny(cleanG)...)
		}
	}
	if id, err := strconv.Atoi(idOrRole); err == nil {
		query += " AND auth_group.id = ?"
		args = append(args, id)
	} else {
		query += " AND auth_group.role = ?"
		args = append(args, idOrRole)
	}
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getGroup: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

func groupAuthClause(query string, args []any, idOrRole string, groups []string, isManager bool) (string, []any) {
	if isManager {
		query += "auth_group.id > 0"
	} else {
		cleanG := cleanGroups(groups)
		if len(cleanG) == 0 {
			query += "1=0"
		} else {
			query += "auth_group.role IN (" + Placeholders(len(cleanG)) + ")"
			args = append(args, stringsToAny(cleanG)...)
		}
	}
	if id, err := strconv.Atoi(idOrRole); err == nil {
		query += " AND auth_group.id = ?"
		args = append(args, id)
	} else {
		query += " AND auth_group.role = ?"
		args = append(args, idOrRole)
	}
	return query, args
}

func (oDb *DB) GetGroupApps(ctx context.Context, idOrRole string, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getGroupApps: no select expressions")
	}
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") +
		" FROM apps" +
		" JOIN apps_responsibles ON apps.id = apps_responsibles.app_id" +
		" JOIN auth_group ON auth_group.id = apps_responsibles.group_id" +
		" WHERE "
	query, args := groupAuthClause(query, []any{}, idOrRole, p.Groups, p.IsManager)
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("apps.app, apps.id")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getGroupApps: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

func (oDb *DB) GetGroupNodes(ctx context.Context, idOrRole string, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getGroupNodes: no select expressions")
	}
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") +
		" FROM nodes" +
		" JOIN auth_group ON nodes.team_responsible = auth_group.role" +
		" WHERE "
	query, args := groupAuthClause(query, []any{}, idOrRole, p.Groups, p.IsManager)
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("nodes.nodename, nodes.node_id")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getGroupNodes: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

func (oDb *DB) GetGroupServices(ctx context.Context, idOrRole string, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getGroupServices: no select expressions")
	}
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") +
		" FROM services" +
		" JOIN apps ON services.svc_app = apps.app" +
		" JOIN apps_responsibles ON apps.id = apps_responsibles.app_id" +
		" JOIN auth_group ON auth_group.id = apps_responsibles.group_id" +
		" WHERE "
	query, args := groupAuthClause(query, []any{}, idOrRole, p.Groups, p.IsManager)
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	} else {
		query += " GROUP BY services.id"
	}
	query += " " + p.OrderByClause("services.svcname")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getGroupServices: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

func groupMembershipClause(query string, args []any, idOrRole string, groups []string) (string, []any) {
	cleanG := cleanGroups(groups)
	if len(cleanG) == 0 {
		query += "1=0"
	} else {
		query += "auth_group.role IN (" + Placeholders(len(cleanG)) + ")"
		args = append(args, stringsToAny(cleanG)...)
	}
	if id, err := strconv.Atoi(idOrRole); err == nil {
		query += " AND auth_group.id = ?"
		args = append(args, id)
	} else {
		query += " AND auth_group.role = ?"
		args = append(args, idOrRole)
	}
	return query, args
}

func (oDb *DB) GetGroupModulesets(ctx context.Context, idOrRole string, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getGroupModulesets: no select expressions")
	}
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") +
		" FROM comp_moduleset" +
		" JOIN comp_moduleset_team_publication ON comp_moduleset_team_publication.modset_id = comp_moduleset.id" +
		" JOIN auth_group ON auth_group.id = comp_moduleset_team_publication.group_id" +
		" WHERE "
	query, args := groupMembershipClause(query, []any{}, idOrRole, p.Groups)
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("comp_moduleset.modset_name, comp_moduleset.id")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getGroupModulesets: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

func (oDb *DB) GetGroupRulesets(ctx context.Context, idOrRole string, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getGroupRulesets: no select expressions")
	}
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") +
		" FROM comp_rulesets" +
		" JOIN comp_ruleset_team_publication ON comp_ruleset_team_publication.ruleset_id = comp_rulesets.id" +
		" JOIN auth_group ON auth_group.id = comp_ruleset_team_publication.group_id" +
		" WHERE "
	query, args := groupMembershipClause(query, []any{}, idOrRole, p.Groups)
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("comp_rulesets.ruleset_name, comp_rulesets.id")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getGroupRulesets: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

func (oDb *DB) GetGroupUsers(ctx context.Context, idOrRole string, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getGroupUsers: no select expressions")
	}
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") +
		" FROM auth_user" +
		" JOIN auth_membership ON auth_user.id = auth_membership.user_id" +
		" JOIN auth_group ON auth_group.id = auth_membership.group_id" +
		" WHERE "
	query, args := groupAuthClause(query, []any{}, idOrRole, p.Groups, p.IsManager)
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("auth_user.email, auth_user.id")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getGroupUsers: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

func (oDb *DB) GroupForDelete(ctx context.Context, idOrRole string, userGroupIDs []int64, isManager bool) (*AuthGroup, error) {
	query := "SELECT id, role, privilege, COALESCE(description, '') FROM auth_group WHERE "
	args := []any{}
	if id, err := strconv.Atoi(idOrRole); err == nil {
		query += "id = ?"
		args = append(args, id)
	} else {
		query += "role = ?"
		args = append(args, idOrRole)
	}
	if isManager {
		query += " AND id > 0"
	} else {
		if len(userGroupIDs) == 0 {
			return nil, nil
		}
		clause, inArgs := inClause("id", toAnyInt64Slice(userGroupIDs))
		query += " AND " + clause
		query += " AND privilege = 'F'"
		args = append(args, inArgs...)
	}
	query += " LIMIT 1"

	var (
		g         AuthGroup
		role      sql.NullString
		privilege sql.NullString
	)
	err := oDb.DB.QueryRowContext(ctx, query, args...).Scan(&g.ID, &role, &privilege, &g.Description)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("GroupForDelete: %w", err)
	}
	if role.Valid {
		g.Role = role.String
	}
	if privilege.Valid {
		g.Privilege = privilege.String == "T"
	}
	return &g, nil
}

func (oDb *DB) DeleteGroupCascade(ctx context.Context, groupID int64) error {
	if _, err := oDb.ExecContext(ctx, "DELETE FROM auth_group WHERE id = ?", groupID); err != nil {
		return fmt.Errorf("DeleteGroupCascade auth_group: %w", err)
	}
	oDb.SetChange("auth_group")

	tables := []string{
		"auth_membership",
		"apps_responsibles",
		"forms_team_responsible",
		"forms_team_publication",
		"comp_moduleset_team_responsible",
		"comp_moduleset_team_publication",
		"comp_ruleset_team_responsible",
		"comp_ruleset_team_publication",
	}
	for _, t := range tables {
		query := "DELETE FROM " + t + " WHERE group_id = ?"
		if _, err := oDb.ExecContext(ctx, query, groupID); err != nil {
			return fmt.Errorf("DeleteGroupCascade %s: %w", t, err)
		}
		oDb.SetChange(t)
	}
	return nil
}

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

// lists the menu entries hidden for the given group.
func (oDb *DB) GetGroupHiddenMenuEntries(ctx context.Context, groupID int64, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getGroupHiddenMenuEntries: no select expressions")
	}
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") +
		" FROM group_hidden_menu_entries WHERE group_hidden_menu_entries.group_id = ?"
	args := []any{groupID}
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("group_hidden_menu_entries.menu_entry, group_hidden_menu_entries.id")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getGroupHiddenMenuEntries: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

func (oDb *DB) GetAllHiddenMenuEntries(ctx context.Context, groupIDs []int64, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getAllHiddenMenuEntries: no select expressions")
	}
	if groupIDs != nil && len(groupIDs) == 0 {
		return []map[string]any{}, nil
	}
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") + " FROM group_hidden_menu_entries"
	var args []any
	if groupIDs != nil {
		query += " WHERE group_hidden_menu_entries.group_id IN (" + Placeholders(len(groupIDs)) + ")"
		for _, id := range groupIDs {
			args = append(args, id)
		}
	}
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("group_hidden_menu_entries.group_id, group_hidden_menu_entries.menu_entry, group_hidden_menu_entries.id")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getAllHiddenMenuEntries: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// reports whether the menu entry is already hidden for the group.
func (oDb *DB) GroupHiddenMenuEntryExists(ctx context.Context, groupID int64, menuEntry string) (bool, error) {
	const query = "SELECT 1 FROM group_hidden_menu_entries WHERE group_id = ? AND menu_entry = ? LIMIT 1"
	var x int
	err := oDb.DB.QueryRowContext(ctx, query, groupID, menuEntry).Scan(&x)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("GroupHiddenMenuEntryExists: %w", err)
	default:
		return true, nil
	}
}

// hides a menu entry for the group.
func (oDb *DB) InsertGroupHiddenMenuEntry(ctx context.Context, groupID int64, menuEntry string) error {
	const query = "INSERT INTO group_hidden_menu_entries (group_id, menu_entry) VALUES (?, ?)"
	if _, err := oDb.DB.ExecContext(ctx, query, groupID, menuEntry); err != nil {
		return fmt.Errorf("insertGroupHiddenMenuEntry: %w", err)
	}
	oDb.SetChange("group_hidden_menu_entries")
	return nil
}

// unhides a menu entry for the group and returns the number of deleted rows.
func (oDb *DB) DeleteGroupHiddenMenuEntry(ctx context.Context, groupID int64, menuEntry string) (int64, error) {
	const query = "DELETE FROM group_hidden_menu_entries WHERE group_id = ? AND menu_entry = ?"
	res, err := oDb.DB.ExecContext(ctx, query, groupID, menuEntry)
	if err != nil {
		return 0, fmt.Errorf("deleteGroupHiddenMenuEntry: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("deleteGroupHiddenMenuEntry rowsAffected: %w", err)
	}
	if n > 0 {
		oDb.SetChange("group_hidden_menu_entries")
	}
	return n, nil
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
