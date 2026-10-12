package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
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

// compLogFrom joins the node and the service of a run, for their names.
const compLogFrom = "comp_log" +
	" LEFT JOIN nodes ON nodes.node_id = comp_log.node_id" +
	" LEFT JOIN services ON services.svc_id = comp_log.svc_id"

// GetComplianceLogs lists the compliance module runs (check, fixable, fix) of the
// nodes the caller may see, the most recent first; one when id is set. A non-nil
// fsetNodeIDs restricts the runs to those nodes, the ones a filterset selects,
// as apply_filters_id() does on the node of a run.
func (oDb *DB) GetComplianceLogs(ctx context.Context, id *int64, fsetNodeIDs []string, p ListParams) ([]map[string]any, error) {
	cond, args := compPublishedNodesCond("comp_log.node_id", p.Groups, p.IsManager)
	conds := []string{cond}
	if id != nil {
		conds = append(conds, "comp_log.id = ?")
		args = append(args, *id)
	}
	if fsetNodeIDs != nil {
		if len(fsetNodeIDs) == 0 {
			conds = append(conds, "1=0")
		} else {
			conds = append(conds, "comp_log.node_id IN ("+Placeholders(len(fsetNodeIDs))+")")
			for _, nodeID := range fsetNodeIDs {
				args = append(args, nodeID)
			}
		}
	}
	return oDb.listQuery(ctx, "getComplianceLogs", compLogFrom, conds, args, "comp_log.id DESC", p)
}

// compStatusFrom joins the node and the service of a check run, for their names.
const compStatusFrom = "comp_status" +
	" LEFT JOIN nodes ON nodes.node_id = comp_status.node_id" +
	" LEFT JOIN services ON services.svc_id = comp_status.svc_id"

// GetComplianceStatus lists the last check run of each module-node-service tuple,
// for the nodes the caller may see; one when id is set.
func (oDb *DB) GetComplianceStatus(ctx context.Context, id *int64, p ListParams) ([]map[string]any, error) {
	cond, args := compPublishedNodesCond("comp_status.node_id", p.Groups, p.IsManager)
	conds := []string{cond}
	if id != nil {
		conds = append(conds, "comp_status.id = ?")
		args = append(args, *id)
	}
	return oDb.listQuery(ctx, "getComplianceStatus", compStatusFrom, conds, args, "comp_status.id DESC", p)
}

// CompStatusObsoleteAfter is the age past which a check run no longer tells the
// state of its module: counted as obsolete, whatever its status, as the
// historical collector's compliance status views do.
const CompStatusObsoleteAfter = "7 DAY"

// CompStatusCounts are the check runs of a selection by state: ok, not ok and not
// applicable among the recent ones, obsolete past CompStatusObsoleteAfter. Total
// is their sum, so a recent run of any other status (a killed module) is left out,
// as in the historical views; Pct is the share of ok and not applicable runs.
type CompStatusCounts struct {
	Total int64
	OK    int64
	NOK   int64
	NA    int64
	Obs   int64
	Pct   *int64
}

// CompStatusGroup are the counts of the runs of one module, node or service.
type CompStatusGroup struct {
	ID   string
	Name string
	CompStatusCounts
}

// compStatusCountExprs are the SELECT expressions of CompStatusCounts.
const compStatusCountExprs = "" +
	"COALESCE(SUM(comp_status.run_date >= NOW() - INTERVAL " + CompStatusObsoleteAfter + " AND comp_status.run_status = 0), 0)," +
	" COALESCE(SUM(comp_status.run_date >= NOW() - INTERVAL " + CompStatusObsoleteAfter + " AND comp_status.run_status = 1), 0)," +
	" COALESCE(SUM(comp_status.run_date >= NOW() - INTERVAL " + CompStatusObsoleteAfter + " AND comp_status.run_status = 2), 0)," +
	" COALESCE(SUM(comp_status.run_date < NOW() - INTERVAL " + CompStatusObsoleteAfter + "), 0)"

func (c *CompStatusCounts) complete() {
	c.Total = c.OK + c.NOK + c.NA + c.Obs
	if c.Total > 0 {
		pct := (c.OK + c.NA) * 100 / c.Total
		c.Pct = &pct
	}
}

// ComplianceStatusSummary counts the check runs the caller may see, narrowed by
// the filters of p, overall and, when by is "module", "node" or "service", per
// group of that kind: the least compliant first, then the largest, then by name,
// as the historical comp_mod_status, comp_node_status and comp_svc_status
// tables. The runs of no service are left out of the service groups.
func (oDb *DB) ComplianceStatusSummary(ctx context.Context, by string, p ListParams) (CompStatusCounts, []CompStatusGroup, error) {
	cond, args := compPublishedNodesCond("comp_status.node_id", p.Groups, p.IsManager)
	conds := []string{cond}
	filterConds, filterArgs := p.FilterConditions()
	conds = append(conds, filterConds...)
	args = append(args, filterArgs...)
	where := " WHERE " + strings.Join(conds, " AND ")

	var totals CompStatusCounts
	query := "SELECT " + compStatusCountExprs + " FROM " + compStatusFrom + where
	if err := oDb.DB.QueryRowContext(ctx, query, args...).Scan(&totals.OK, &totals.NOK, &totals.NA, &totals.Obs); err != nil {
		return totals, nil, fmt.Errorf("complianceStatusSummary: %w", err)
	}
	totals.complete()

	var key, name, extra string
	switch by {
	case "":
		return totals, nil, nil
	case "module":
		key, name = "comp_status.run_module", "comp_status.run_module"
	case "node":
		key, name = "comp_status.node_id", "COALESCE(MAX(nodes.nodename), '')"
	case "service":
		key, name = "comp_status.svc_id", "COALESCE(MAX(services.svcname), '')"
		extra = " AND COALESCE(comp_status.svc_id, '') <> ''"
	default:
		return totals, nil, fmt.Errorf("complianceStatusSummary: unknown grouping %q", by)
	}
	query = "SELECT COALESCE(" + key + ", ''), " + name + ", " + compStatusCountExprs +
		" FROM " + compStatusFrom + where + extra + " GROUP BY " + key
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return totals, nil, fmt.Errorf("complianceStatusSummary: %w", err)
	}
	defer func() { _ = rows.Close() }()
	groups := []CompStatusGroup{}
	for rows.Next() {
		var g CompStatusGroup
		if err := rows.Scan(&g.ID, &g.Name, &g.OK, &g.NOK, &g.NA, &g.Obs); err != nil {
			return totals, nil, fmt.Errorf("complianceStatusSummary: %w", err)
		}
		g.complete()
		groups = append(groups, g)
	}
	if err := rows.Err(); err != nil {
		return totals, nil, fmt.Errorf("complianceStatusSummary: %w", err)
	}
	sortCompStatusGroups(groups)
	return totals, groups, nil
}

// sortCompStatusGroups orders the groups the least compliant first, then the
// largest, then by name.
func sortCompStatusGroups(groups []CompStatusGroup) {
	pct := func(g CompStatusGroup) int64 {
		if g.Pct == nil {
			return 101
		}
		return *g.Pct
	}
	sort.SliceStable(groups, func(i, j int) bool {
		a, b := groups[i], groups[j]
		if pct(a) != pct(b) {
			return pct(a) < pct(b)
		}
		if a.Total != b.Total {
			return a.Total > b.Total
		}
		return a.Name < b.Name
	})
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
