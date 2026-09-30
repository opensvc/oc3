package cdb

import (
	"context"
)

// compPublishedServicesCond restricts a svc_id column to the services the caller
// may see, as q_filter(svc_field=...): those whose app is published to one of the
// caller's groups; every service for a manager.
func compPublishedServicesCond(svcCol string, groups []string, isManager bool) (string, []any) {
	if isManager {
		return "1=1", nil
	}
	cond, args := rolesCond("ag", groups, false)
	return svcCol + " IN (SELECT s.svc_id FROM services s" +
		" JOIN apps a ON a.app = s.svc_app" +
		" JOIN apps_publications ap ON ap.app_id = a.id" +
		" JOIN auth_group ag ON ag.id = ap.group_id" +
		" WHERE " + cond + ")", args
}

// GetCompObjectNodes lists the nodes a ruleset or a moduleset is attached to,
// or with candidates the nodes it may be attached to: those whose responsible
// team is one of its publication groups, not attached yet. Both are restricted
// to the nodes the caller may see.
func (oDb *DB) GetCompObjectNodes(ctx context.Context, k CompKind, objID int64, candidates bool, p ListParams) ([]map[string]any, error) {
	attached := "SELECT node_id FROM " + k.NodesTable + " WHERE " + k.FK + " = ?"
	var conds []string
	var args []any
	if candidates {
		conds = append(conds,
			"nodes.team_responsible IN (SELECT ag.role FROM "+k.TeamPrefix+"publication tp"+
				" JOIN auth_group ag ON ag.id = tp.group_id WHERE tp."+k.FK+" = ?)",
			"nodes.node_id NOT IN ("+attached+")")
		args = append(args, objID, objID)
	} else {
		conds = append(conds, "nodes.node_id IN ("+attached+")")
		args = append(args, objID)
	}
	cond, condArgs := compPublishedNodesCond("nodes.node_id", p.Groups, p.IsManager)
	conds = append(conds, cond)
	args = append(args, condArgs...)
	return oDb.listQuery(ctx, "getCompObjectNodes", "nodes", conds, args, "nodes.nodename", p)
}

// GetCompObjectServices lists the services a ruleset or a moduleset is attached
// to, as the service itself or with slave as its encapsulated service; or with
// candidates the services it may be attached to: those whose app has one of its
// publication groups among its responsibles, not attached yet. Both are
// restricted to the services the caller may see.
func (oDb *DB) GetCompObjectServices(ctx context.Context, k CompKind, objID int64, slave, candidates bool, p ListParams) ([]map[string]any, error) {
	attached := "SELECT svc_id FROM " + k.ServicesTable + " WHERE " + k.FK + " = ? AND " + compSlaveCond("slave", slave)
	var conds []string
	var args []any
	if candidates {
		conds = append(conds,
			"services.svc_app IN (SELECT a.app FROM apps a"+
				" JOIN apps_responsibles ar ON ar.app_id = a.id"+
				" JOIN "+k.TeamPrefix+"publication tp ON tp.group_id = ar.group_id"+
				" WHERE tp."+k.FK+" = ?)",
			"services.svc_id NOT IN ("+attached+")")
		args = append(args, objID, objID)
	} else {
		conds = append(conds, "services.svc_id IN ("+attached+")")
		args = append(args, objID)
	}
	cond, condArgs := compPublishedServicesCond("services.svc_id", p.Groups, p.IsManager)
	conds = append(conds, cond)
	args = append(args, condArgs...)
	return oDb.listQuery(ctx, "getCompObjectServices", "services", conds, args, "services.svcname", p)
}
