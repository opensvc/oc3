package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// compPublishedNodesCond restricts a node_id column to the nodes the caller may
// see, as q_filter(node_field=...) of the historical collector: those whose app is
// published to one of the caller's groups; every node for a manager.
func compPublishedNodesCond(nodeCol string, groups []string, isManager bool) (string, []any) {
	if isManager {
		return "1=1", nil
	}
	clean := cleanGroups(groups)
	if len(clean) == 0 {
		return "1=0", nil
	}
	args := make([]any, len(clean))
	for i, g := range clean {
		args[i] = g
	}
	return nodeCol + " IN (SELECT n.node_id FROM nodes n" +
		" JOIN apps a ON a.app = n.app" +
		" JOIN apps_publications ap ON ap.app_id = a.id" +
		" JOIN auth_group ag ON ag.id = ap.group_id" +
		" WHERE ag.role IN (" + Placeholders(len(clean)) + "))", args
}

// GetComplianceLogs lists the compliance module runs (check, fixable, fix) of the
// nodes the caller may see, the most recent first; one when id is set.
func (oDb *DB) GetComplianceLogs(ctx context.Context, id *int64, p ListParams) ([]map[string]any, error) {
	cond, args := compPublishedNodesCond("comp_log.node_id", p.Groups, p.IsManager)
	conds := []string{cond}
	if id != nil {
		conds = append(conds, "comp_log.id = ?")
		args = append(args, *id)
	}
	return oDb.listQuery(ctx, "getComplianceLogs", "comp_log", conds, args, "comp_log.id DESC", p)
}

// GetComplianceStatus lists the last check run of each module-node-service tuple,
// for the nodes the caller may see; one when id is set.
func (oDb *DB) GetComplianceStatus(ctx context.Context, id *int64, p ListParams) ([]map[string]any, error) {
	cond, args := compPublishedNodesCond("comp_status.node_id", p.Groups, p.IsManager)
	conds := []string{cond}
	if id != nil {
		conds = append(conds, "comp_status.id = ?")
		args = append(args, *id)
	}
	return oDb.listQuery(ctx, "getComplianceStatus", "comp_status", conds, args, "comp_status.id DESC", p)
}

// CompStatusRun is the last check run of a module on a node, or on a service
// instance on that node.
type CompStatusRun struct {
	ID        int64
	RunModule string
	NodeID    string
	SvcID     string
	Nodename  string
	Svcname   string
}

// CompStatusRunVisible returns a check run of a node the caller may see, nil when
// there is none.
func (oDb *DB) CompStatusRunVisible(ctx context.Context, id int64, groups []string, isManager bool) (*CompStatusRun, error) {
	cond, args := compPublishedNodesCond("comp_status.node_id", groups, isManager)
	query := "SELECT comp_status.id, comp_status.run_module, COALESCE(comp_status.node_id, ''), COALESCE(comp_status.svc_id, '')," +
		" COALESCE(nodes.nodename, ''), COALESCE(services.svcname, '')" +
		" FROM comp_status" +
		" LEFT JOIN nodes ON nodes.node_id = comp_status.node_id" +
		" LEFT JOIN services ON services.svc_id = comp_status.svc_id" +
		" WHERE comp_status.id = ? AND " + cond
	var r CompStatusRun
	err := oDb.DB.QueryRowContext(ctx, query, append([]any{id}, args...)...).
		Scan(&r.ID, &r.RunModule, &r.NodeID, &r.SvcID, &r.Nodename, &r.Svcname)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("compStatusRunVisible: %w", err)
	}
	return &r, nil
}

// DeleteCompStatusRun deletes a check run.
func (oDb *DB) DeleteCompStatusRun(ctx context.Context, id int64) error {
	if _, err := oDb.DB.ExecContext(ctx, "DELETE FROM comp_status WHERE id = ?", id); err != nil {
		return fmt.Errorf("deleteCompStatusRun: %w", err)
	}
	oDb.SetChange("comp_status")
	return nil
}
