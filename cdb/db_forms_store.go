package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// listQuery runs a hand-built list query: the select expressions of the request
// over from, the conditions and the filters ANDed, then grouping, sort and page.
func (oDb *DB) listQuery(ctx context.Context, name, from string, conds []string, args []any, defaultOrder string, p ListParams) ([]map[string]any, error) {
	if len(p.SelectExprs) == 0 {
		return nil, fmt.Errorf("%s: no select expressions", name)
	}
	filterConds, filterArgs := p.FilterConditions()
	conds = append(conds, filterConds...)
	args = append(args, filterArgs...)
	query := "SELECT " + strings.Join(p.SelectExprs, ", ") + " FROM " + from
	if len(conds) > 0 {
		query += " WHERE " + strings.Join(conds, " AND ")
	}
	if gb := p.GroupByClause(""); gb != "" {
		query += " " + gb
	}
	query += " " + p.OrderByClause(defaultOrder)
	query, args = appendLimitOffset(query, args, p.Limit, p.Offset)
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	defer func() { _ = rows.Close() }()
	return scanRowsToMaps(rows, p.Props, p.TypeHints)
}

// GetFormsRevisions lists the form revisions, one when idOrMD5 is set: a
// numeric value is an id, anything else a md5, as the historical handler does.
func (oDb *DB) GetFormsRevisions(ctx context.Context, idOrMD5 *string, p ListParams) ([]map[string]any, error) {
	conds := []string{"forms_revisions.id > 0"}
	var args []any
	if idOrMD5 != nil {
		if _, err := strconv.ParseInt(*idOrMD5, 10, 64); err == nil {
			conds = append(conds, "forms_revisions.id = ?")
		} else {
			conds = append(conds, "forms_revisions.form_md5 = ?")
		}
		args = append(args, *idOrMD5)
	}
	return oDb.listQuery(ctx, "getFormsRevisions", "forms_revisions", conds, args, "forms_revisions.id", p)
}

// formsStoreFrom joins the stored forms to the revision they were submitted
// with, as the v_forms_store view of the historical collector.
const formsStoreFrom = "forms_store JOIN forms_revisions ON forms_revisions.form_md5 = forms_store.form_md5"

// GetFormsStore lists the forms stored by workflows, one when storeID is set.
func (oDb *DB) GetFormsStore(ctx context.Context, storeID *int64, p ListParams) ([]map[string]any, error) {
	conds := []string{"forms_store.id > 0"}
	var args []any
	if storeID != nil {
		conds = append(conds, "forms_store.id = ?")
		args = append(args, *storeID)
	}
	return oDb.listQuery(ctx, "getFormsStore", formsStoreFrom, conds, args, "forms_store.id", p)
}

// workflowsFrom joins the form revision a workflow started from, for its name,
// folder and definition; a workflow whose revision is missing is still listed.
const workflowsFrom = "workflows LEFT JOIN forms_revisions ON forms_revisions.form_md5 = workflows.form_md5"

// GetWorkflows lists the workflows, one when id is set.
func (oDb *DB) GetWorkflows(ctx context.Context, id *int64, p ListParams) ([]map[string]any, error) {
	conds := []string{"workflows.id > 0"}
	var args []any
	if id != nil {
		conds = append(conds, "workflows.id = ?")
		args = append(args, *id)
	}
	return oDb.listQuery(ctx, "getWorkflows", workflowsFrom, conds, args, "workflows.id", p)
}

// WorkflowIDByHead returns the workflow started by a stored form.
func (oDb *DB) WorkflowIDByHead(ctx context.Context, headID int64) (int64, bool, error) {
	var id int64
	err := oDb.DB.QueryRowContext(ctx, "SELECT id FROM workflows WHERE form_head_id = ?", headID).Scan(&id)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, false, nil
	case err != nil:
		return 0, false, fmt.Errorf("workflowIDByHead: %w", err)
	}
	return id, true, nil
}

// FormResultsAccess is the caller of a form results request, for the access
// filter of q_filter(user_field, node_field, svc_field).
type FormResultsAccess struct {
	IsManager bool
	UserID    *int64
	NodeID    string
}

// FormOutputResults returns the results structure of a submission, as stored,
// when the caller may read it: a manager reads every result; a user those
// submitted by the users sharing one of their organization groups; a node
// those of its services, and its own.
func (oDb *DB) FormOutputResults(ctx context.Context, id int64, access FormResultsAccess) (string, bool, error) {
	query := "SELECT COALESCE(results, '') FROM form_output_results WHERE id = ?"
	args := []any{id}
	switch {
	case access.NodeID != "":
		query += " AND (node_id = ? OR svc_id IN (SELECT svc_id FROM svcmon WHERE node_id = ?))"
		args = append(args, access.NodeID, access.NodeID)
	case access.IsManager:
	case access.UserID != nil:
		query += " AND user_id IN (SELECT DISTINCT m.user_id FROM auth_membership m" +
			" WHERE m.group_id IN (SELECT g.id FROM auth_group g JOIN auth_membership am ON am.group_id = g.id" +
			" WHERE am.user_id = ? AND g.privilege = 'F'))"
		args = append(args, *access.UserID)
	default:
		return "", false, nil
	}
	var results string
	err := oDb.DB.QueryRowContext(ctx, query, args...).Scan(&results)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "", false, nil
	case err != nil:
		return "", false, fmt.Errorf("formOutputResults: %w", err)
	}
	return results, true, nil
}

// InsertFormOutputResults stores the results structure of a new submission.
func (oDb *DB) InsertFormOutputResults(ctx context.Context, userID *int64, nodeID, svcID, results string) (int64, error) {
	res, err := oDb.ExecContext(ctx, "INSERT INTO form_output_results (user_id, node_id, svc_id, results) VALUES (?, ?, ?, ?)",
		userID, nodeID, svcID, results)
	if err != nil {
		return 0, fmt.Errorf("insertFormOutputResults: %w", err)
	}
	oDb.SetChange("form_output_results")
	return res.LastInsertId()
}

// UpdateFormOutputResults replaces the results structure of a submission.
func (oDb *DB) UpdateFormOutputResults(ctx context.Context, id int64, results string) error {
	if _, err := oDb.ExecContext(ctx, "UPDATE form_output_results SET results = ? WHERE id = ?", results, id); err != nil {
		return fmt.Errorf("updateFormOutputResults: %w", err)
	}
	oDb.SetChange("form_output_results")
	return nil
}

// ReadFormOutputResults returns the stored results structure, without access
// filter: for the submission engine, which owns it.
func (oDb *DB) ReadFormOutputResults(ctx context.Context, id int64) (string, error) {
	var results sql.NullString
	if err := oDb.DB.QueryRowContext(ctx, "SELECT results FROM form_output_results WHERE id = ?", id).Scan(&results); err != nil {
		return "", fmt.Errorf("readFormOutputResults: %w", err)
	}
	return results.String, nil
}
