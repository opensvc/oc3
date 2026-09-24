package cdb

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// TableColumns returns the columns of a table of the collector database, none
// when the table does not exist.
func (oDb *DB) TableColumns(ctx context.Context, table string) ([]string, error) {
	rows, err := oDb.DB.QueryContext(ctx, "SELECT column_name FROM information_schema.columns"+
		" WHERE table_schema = DATABASE() AND table_name = ? ORDER BY ordinal_position", table)
	if err != nil {
		return nil, fmt.Errorf("tableColumns: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var cols []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, fmt.Errorf("tableColumns: %w", err)
		}
		cols = append(cols, col)
	}
	return cols, rows.Err()
}

// InsertRow inserts a row in a table, the keys checked against its columns by
// the caller.
func (oDb *DB) InsertRow(ctx context.Context, table string, row map[string]any) error {
	keys := sortedKeys(row)
	quoted := make([]string, len(keys))
	args := make([]any, len(keys))
	for i, k := range keys {
		quoted[i] = "`" + strings.ReplaceAll(k, "`", "") + "`"
		args[i] = row[k]
	}
	query := "INSERT INTO `" + strings.ReplaceAll(table, "`", "") + "` (" + strings.Join(quoted, ", ") +
		") VALUES (" + Placeholders(len(keys)) + ")"
	if _, err := oDb.ExecContext(ctx, query, args...); err != nil {
		return err
	}
	oDb.SetChange(table)
	return nil
}

// UserPrimaryOrgGroupRole returns the role of the user's primary group when it
// is not a privilege group, as user_primary_group() does.
func (oDb *DB) UserPrimaryOrgGroupRole(ctx context.Context, userID int64) (string, bool, error) {
	var role sql.NullString
	err := oDb.DB.QueryRowContext(ctx, "SELECT auth_group.role FROM auth_group"+
		" JOIN auth_membership ON auth_membership.group_id = auth_group.id"+
		" WHERE auth_membership.user_id = ? AND auth_membership.primary_group = 'T'"+
		" AND auth_group.privilege = 'F' LIMIT 1", userID).Scan(&role)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "", false, nil
	case err != nil:
		return "", false, fmt.Errorf("userPrimaryOrgGroupRole: %w", err)
	}
	return role.String, role.Valid, nil
}

// InsertFormRevisionMD5 records the definition a form is submitted with, once
// per content, and returns its md5, as insert_form_md5() does.
func (oDb *DB) InsertFormRevisionMD5(ctx context.Context, f *Form) (string, error) {
	sum := md5.Sum([]byte(f.Yaml))
	formMD5 := hex.EncodeToString(sum[:])
	var one int
	err := oDb.DB.QueryRowContext(ctx, "SELECT 1 FROM forms_revisions WHERE form_md5 = ?", formMD5).Scan(&one)
	switch {
	case err == nil:
		return formMD5, nil
	case !errors.Is(err, sql.ErrNoRows):
		return "", fmt.Errorf("insertFormRevisionMD5: %w", err)
	}
	if _, err := oDb.ExecContext(ctx, "INSERT INTO forms_revisions (form_id, form_yaml, form_folder, form_name, form_md5)"+
		" VALUES (?, ?, ?, ?, ?)", f.ID, f.Yaml, f.Folder, f.Name, formMD5); err != nil {
		return "", fmt.Errorf("insertFormRevisionMD5: %w", err)
	}
	oDb.SetChange("forms_revisions")
	return formMD5, nil
}

// StoredFormLink is what a workflow step needs from a stored form.
type StoredFormLink struct {
	ID         int64
	PrevID     *int64
	NextID     *int64
	Submitter  string
	SubmitDate string
}

// StoredFormLinkByID returns the links of a stored form, nil when it does not
// exist.
func (oDb *DB) StoredFormLinkByID(ctx context.Context, id int64) (*StoredFormLink, error) {
	var l StoredFormLink
	var prev, next sql.NullInt64
	err := oDb.DB.QueryRowContext(ctx, "SELECT id, form_prev_id, form_next_id, form_submitter,"+
		" DATE_FORMAT(form_submit_date, '%Y-%m-%d %H:%i:%s') FROM forms_store WHERE id = ?", id).
		Scan(&l.ID, &prev, &next, &l.Submitter, &l.SubmitDate)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("storedFormLinkByID: %w", err)
	}
	if prev.Valid {
		l.PrevID = &prev.Int64
	}
	if next.Valid {
		l.NextID = &next.Int64
	}
	return &l, nil
}

// StoredFormInsert is a new step of a workflow.
type StoredFormInsert struct {
	MD5       string
	Submitter string
	Assignee  string
	Date      string
	PrevID    *int64
	NextID    *int64
	HeadID    *int64
	Data      string
	ResultsID int64
}

// InsertStoredForm stores a submitted form as a workflow step and returns its id.
func (oDb *DB) InsertStoredForm(ctx context.Context, s StoredFormInsert) (int64, error) {
	res, err := oDb.ExecContext(ctx, "INSERT INTO forms_store (form_md5, form_submitter, form_assignee,"+
		" form_submit_date, form_prev_id, form_next_id, form_head_id, form_data, results_id)"+
		" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
		s.MD5, s.Submitter, s.Assignee, s.Date, s.PrevID, s.NextID, s.HeadID, s.Data, s.ResultsID)
	if err != nil {
		return 0, fmt.Errorf("insertStoredForm: %w", err)
	}
	oDb.SetChange("forms_store")
	return res.LastInsertId()
}

// SetStoredFormNext links a workflow step to the next one.
func (oDb *DB) SetStoredFormNext(ctx context.Context, id, nextID int64) error {
	if _, err := oDb.ExecContext(ctx, "UPDATE forms_store SET form_next_id = ? WHERE id = ?", nextID, id); err != nil {
		return fmt.Errorf("setStoredFormNext: %w", err)
	}
	oDb.SetChange("forms_store")
	return nil
}

// SetStoredFormHead makes a stored form the head of its own workflow.
func (oDb *DB) SetStoredFormHead(ctx context.Context, id int64) error {
	if _, err := oDb.ExecContext(ctx, "UPDATE forms_store SET form_head_id = ? WHERE id = ?", id, id); err != nil {
		return fmt.Errorf("setStoredFormHead: %w", err)
	}
	oDb.SetChange("forms_store")
	return nil
}

// WorkflowInsert is a new workflow, or the new state of an existing one.
type WorkflowInsert struct {
	Status       string
	MD5          string
	Steps        int
	LastAssignee string
	LastUpdate   string
	LastFormID   int64
	LastFormName string
	HeadID       int64
	Creator      string
	CreateDate   string
}

// InsertWorkflow creates a workflow and returns its id.
func (oDb *DB) InsertWorkflow(ctx context.Context, w WorkflowInsert) (int64, error) {
	res, err := oDb.ExecContext(ctx, "INSERT INTO workflows (status, form_md5, steps, last_assignee, last_update,"+
		" last_form_id, last_form_name, form_head_id, creator, create_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		w.Status, w.MD5, w.Steps, w.LastAssignee, w.LastUpdate, w.LastFormID, w.LastFormName, w.HeadID, w.Creator, w.CreateDate)
	if err != nil {
		return 0, fmt.Errorf("insertWorkflow: %w", err)
	}
	oDb.SetChange("workflows")
	return res.LastInsertId()
}

// UpdateWorkflowStep records a new step of the workflow started by headID.
func (oDb *DB) UpdateWorkflowStep(ctx context.Context, headID int64, w WorkflowInsert) error {
	if _, err := oDb.ExecContext(ctx, "UPDATE workflows SET status = ?, steps = ?, last_assignee = ?,"+
		" last_form_id = ?, last_form_name = ?, last_update = ? WHERE form_head_id = ?",
		w.Status, w.Steps, w.LastAssignee, w.LastFormID, w.LastFormName, w.LastUpdate, headID); err != nil {
		return fmt.Errorf("updateWorkflowStep: %w", err)
	}
	oDb.SetChange("workflows")
	return nil
}
