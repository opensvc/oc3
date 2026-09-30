package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
)

// Form is a row of the forms table.
type Form struct {
	ID      int64
	Name    string
	Yaml    string
	Author  string
	Created string
	Type    string
	Folder  string
}

// formCols lists the forms columns a caller may set, as the historical collector
// wrote any posted key to the table.
var formCols = map[string]bool{
	"form_name": true, "form_yaml": true, "form_author": true,
	"form_created": true, "form_type": true, "form_folder": true,
}

// IsFormColumn reports whether a posted key is a settable forms column.
func IsFormColumn(key string) bool { return formCols[key] }

// formPublishedCond restricts forms to those published to one of the caller's
// groups, as the historical collector does for a non-manager.
func formPublishedCond(formIDExpr string, groups []string) (string, []any) {
	if len(groups) == 0 {
		return "1=0", nil
	}
	args := make([]any, len(groups))
	for i, g := range groups {
		args[i] = g
	}
	return formIDExpr + " IN (SELECT ftp.form_id FROM forms_team_publication ftp" +
		" JOIN auth_group ag ON ag.id = ftp.group_id WHERE ag.role IN (" + Placeholders(len(groups)) + "))", args
}

// GetForms lists the forms visible to the caller, one form when formID is set.
func (oDb *DB) GetForms(ctx context.Context, formID *int64, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getForms: no select expressions")
	}
	conds := []string{"forms.id > 0"}
	var args []any
	if formID != nil {
		conds = append(conds, "forms.id = ?")
		args = append(args, *formID)
	}
	if !p.IsManager {
		cond, condArgs := formPublishedCond("forms.id", p.Groups)
		conds = append(conds, cond)
		args = append(args, condArgs...)
	}
	filterConds, filterArgs := p.FilterConditions()
	conds = append(conds, filterConds...)
	args = append(args, filterArgs...)

	query := "SELECT " + strings.Join(p.SelectExprs, ", ") + " FROM forms WHERE " + strings.Join(conds, " AND ")
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("forms.form_name")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getForms: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// FormByID returns a form, nil when it does not exist.
func (oDb *DB) FormByID(ctx context.Context, id int64) (*Form, error) {
	var f Form
	var name, yaml, author, typ, folder sql.NullString
	err := oDb.DB.QueryRowContext(ctx, "SELECT id, form_name, form_yaml, form_author, COALESCE(form_created, ''),"+
		" form_type, form_folder FROM forms WHERE id = ?", id).
		Scan(&f.ID, &name, &yaml, &author, &f.Created, &typ, &folder)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("formByID: %w", err)
	}
	f.Name, f.Yaml, f.Author, f.Type, f.Folder = name.String, yaml.String, author.String, typ.String, folder.String
	return &f, nil
}

// FormIDByName returns the id of the form with that name.
func (oDb *DB) FormIDByName(ctx context.Context, name string) (int64, bool, error) {
	var id int64
	err := oDb.DB.QueryRowContext(ctx, "SELECT id FROM forms WHERE form_name = ?", name).Scan(&id)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, false, nil
	case err != nil:
		return 0, false, fmt.Errorf("formIDByName: %w", err)
	}
	return id, true, nil
}

// FormVisible reports whether the form exists and the caller may see it: a
// manager sees every form, others the forms published to one of their groups.
func (oDb *DB) FormVisible(ctx context.Context, formID int64, groups []string, isManager bool) (bool, error) {
	return oDb.formLinked(ctx, "forms_team_publication", formID, groups, isManager)
}

// FormResponsible reports whether the caller is responsible for the form: a
// manager is responsible for every form, others through a responsible group.
func (oDb *DB) FormResponsible(ctx context.Context, formID int64, groups []string, isManager bool) (bool, error) {
	return oDb.formLinked(ctx, "forms_team_responsible", formID, groups, isManager)
}

func (oDb *DB) formLinked(ctx context.Context, table string, formID int64, groups []string, isManager bool) (bool, error) {
	query := "SELECT 1 FROM forms WHERE forms.id = ?"
	args := []any{formID}
	if !isManager {
		if len(groups) == 0 {
			return false, nil
		}
		query += " AND forms.id IN (SELECT t.form_id FROM " + table + " t JOIN auth_group ag ON ag.id = t.group_id" +
			" WHERE ag.role IN (" + Placeholders(len(groups)) + "))"
		for _, g := range groups {
			args = append(args, g)
		}
	}
	var one int
	err := oDb.DB.QueryRowContext(ctx, query+" LIMIT 1", args...).Scan(&one)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("formLinked %s: %w", table, err)
	}
	return true, nil
}

// sortedKeys returns the keys of a column map in a stable order, for the SQL.
func sortedKeys(fields map[string]any) []string {
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// InsertForm creates a form from settable columns and returns its id.
func (oDb *DB) InsertForm(ctx context.Context, fields map[string]any) (int64, error) {
	keys := sortedKeys(fields)
	args := make([]any, len(keys))
	for i, k := range keys {
		if !formCols[k] {
			return 0, fmt.Errorf("insertForm: unknown column %s", k)
		}
		args[i] = fields[k]
	}
	query := "INSERT INTO forms (" + strings.Join(keys, ", ") + ") VALUES (" + Placeholders(len(keys)) + ")"
	res, err := oDb.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("insertForm: %w", err)
	}
	oDb.SetChange("forms")
	return res.LastInsertId()
}

// UpdateForm sets settable columns of a form.
func (oDb *DB) UpdateForm(ctx context.Context, id int64, fields map[string]any) error {
	if len(fields) == 0 {
		return nil
	}
	keys := sortedKeys(fields)
	sets := make([]string, len(keys))
	args := make([]any, 0, len(keys)+1)
	for i, k := range keys {
		if !formCols[k] {
			return fmt.Errorf("updateForm: unknown column %s", k)
		}
		sets[i] = k + " = ?"
		args = append(args, fields[k])
	}
	args = append(args, id)
	if _, err := oDb.ExecContext(ctx, "UPDATE forms SET "+strings.Join(sets, ", ")+" WHERE id = ?", args...); err != nil {
		return fmt.Errorf("updateForm: %w", err)
	}
	oDb.SetChange("forms")
	return nil
}

// DeleteForm deletes a form with its publications and responsibles.
func (oDb *DB) DeleteForm(ctx context.Context, id int64) error {
	for _, table := range []string{"forms", "forms_team_publication", "forms_team_responsible"} {
		col := "form_id"
		if table == "forms" {
			col = "id"
		}
		if _, err := oDb.ExecContext(ctx, "DELETE FROM "+table+" WHERE "+col+" = ?", id); err != nil {
			return fmt.Errorf("deleteForm %s: %w", table, err)
		}
		oDb.SetChange(table)
	}
	return nil
}

// FormTeamTable names the table of a form/group link: publications or
// responsibles.
type FormTeamTable string

const (
	FormPublications FormTeamTable = "forms_team_publication"
	FormResponsibles FormTeamTable = "forms_team_responsible"
)

// FormTeamExists reports whether the form is linked to the group.
func (oDb *DB) FormTeamExists(ctx context.Context, table FormTeamTable, formID, groupID int64) (bool, error) {
	var one int
	err := oDb.DB.QueryRowContext(ctx, "SELECT 1 FROM "+string(table)+" WHERE form_id = ? AND group_id = ? LIMIT 1",
		formID, groupID).Scan(&one)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("formTeamExists: %w", err)
	}
	return true, nil
}

// InsertFormTeam links the form to the group.
func (oDb *DB) InsertFormTeam(ctx context.Context, table FormTeamTable, formID, groupID int64) error {
	if _, err := oDb.ExecContext(ctx, "INSERT INTO "+string(table)+" (form_id, group_id) VALUES (?, ?)", formID, groupID); err != nil {
		return fmt.Errorf("insertFormTeam: %w", err)
	}
	oDb.SetChange(string(table))
	return nil
}

// DeleteFormTeam unlinks the form from the group.
func (oDb *DB) DeleteFormTeam(ctx context.Context, table FormTeamTable, formID, groupID int64) error {
	if _, err := oDb.ExecContext(ctx, "DELETE FROM "+string(table)+" WHERE form_id = ? AND group_id = ?", formID, groupID); err != nil {
		return fmt.Errorf("deleteFormTeam: %w", err)
	}
	oDb.SetChange(string(table))
	return nil
}

// GetFormTeam lists the groups linked to a form, as auth_group rows.
func (oDb *DB) GetFormTeam(ctx context.Context, table FormTeamTable, formID int64, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("getFormTeam: no select expressions")
	}
	conds := []string{"t.form_id = ?"}
	args := []any{formID}
	filterConds, filterArgs := p.FilterConditions()
	conds = append(conds, filterConds...)
	args = append(args, filterArgs...)
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") + " FROM auth_group JOIN " + string(table) +
		" t ON t.group_id = auth_group.id WHERE " + strings.Join(conds, " AND ")
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause("auth_group.role")
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("getFormTeam: %w", err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// UserName returns "First Last" of a user, as the historical collector names the
// author of a change, and the email.
func (oDb *DB) UserName(ctx context.Context, userID int64) (string, string, error) {
	var first, last, email sql.NullString
	err := oDb.DB.QueryRowContext(ctx, "SELECT first_name, last_name, email FROM auth_user WHERE id = ?", userID).
		Scan(&first, &last, &email)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "Unknown", "", nil
	case err != nil:
		return "", "", fmt.Errorf("userName: %w", err)
	}
	return first.String + " " + last.String, email.String, nil
}

// UserEmailByName returns the email of the user named "First Last", as
// email_of() resolves a mail recipient given by name.
func (oDb *DB) UserEmailByName(ctx context.Context, name string) (string, bool, error) {
	rows, err := oDb.DB.QueryContext(ctx, `SELECT email FROM auth_user WHERE CONCAT(first_name, " ", last_name) = ?`, name)
	if err != nil {
		return "", false, fmt.Errorf("userEmailByName: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var emails []string
	for rows.Next() {
		var email sql.NullString
		if err := rows.Scan(&email); err != nil {
			return "", false, fmt.Errorf("userEmailByName: %w", err)
		}
		emails = append(emails, email.String)
	}
	if len(emails) != 1 {
		return "", false, rows.Err()
	}
	return emails[0], true, rows.Err()
}
