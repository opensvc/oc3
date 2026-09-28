package cdb

import (
	"context"
	"fmt"
)

// CompKind names the tables of a compliance object kind that has publication and
// responsible groups: rulesets and modulesets.
type CompKind struct {
	// Name is the kind as the messages name it: "ruleset", "moduleset".
	Name string
	// Table and NameCol are the object table and its name column.
	Table, NameCol string
	// TeamPrefix is the prefix of the group tables, completed by "publication"
	// or "responsible"; FK is their column naming the object.
	TeamPrefix, FK string
}

var (
	CompRulesetKind   = CompKind{Name: "ruleset", Table: "comp_rulesets", NameCol: "ruleset_name", TeamPrefix: "comp_ruleset_team_", FK: "ruleset_id"}
	CompModulesetKind = CompKind{Name: "moduleset", Table: "comp_moduleset", NameCol: "modset_name", TeamPrefix: "comp_moduleset_team_", FK: "modset_id"}
)

// CompTeamTypes are the two kinds of groups of a compliance object.
var CompTeamTypes = []string{"publication", "responsible"}

func (k CompKind) teamTable(gtype string) (string, error) {
	for _, t := range CompTeamTypes {
		if t == gtype {
			return k.TeamPrefix + gtype, nil
		}
	}
	return "", fmt.Errorf("unknown group type %q", gtype)
}

// GetCompTeamGroups lists the publication or responsible groups of a compliance
// object; a non-manager sees only those they are a member of, as the historical
// rest_get_compliance_*_publications and _responsibles.
func (oDb *DB) GetCompTeamGroups(ctx context.Context, k CompKind, objID int64, gtype string, p ListParams) ([]map[string]any, error) {
	table, err := k.teamTable(gtype)
	if err != nil {
		return nil, err
	}
	conds := []string{"auth_group.id IN (SELECT group_id FROM " + table + " WHERE " + k.FK + " = ?)"}
	args := []any{objID}
	if !p.IsManager {
		cond, condArgs := rolesCond("auth_group", p.Groups, false)
		conds = append(conds, cond)
		args = append(args, condArgs...)
	}
	return oDb.listQuery(ctx, "getCompTeamGroups", "auth_group", conds, args, "auth_group.role", p)
}

// CompTeamAttached tells whether a group is a publication or responsible group of
// a compliance object.
func (oDb *DB) CompTeamAttached(ctx context.Context, k CompKind, objID int64, gtype string, groupID int64) (bool, error) {
	table, err := k.teamTable(gtype)
	if err != nil {
		return false, err
	}
	return oDb.exists(ctx, "compTeamAttached", "SELECT 1 FROM "+table+" WHERE "+k.FK+" = ? AND group_id = ?", objID, groupID)
}

// AttachCompTeam makes a group a publication or responsible group of a
// compliance object.
func (oDb *DB) AttachCompTeam(ctx context.Context, k CompKind, objID int64, gtype string, groupID int64) error {
	table, err := k.teamTable(gtype)
	if err != nil {
		return err
	}
	if _, err := oDb.DB.ExecContext(ctx, "INSERT INTO "+table+" ("+k.FK+", group_id) VALUES (?, ?)", objID, groupID); err != nil {
		return fmt.Errorf("attachCompTeam: %w", err)
	}
	oDb.SetChange(table)
	return nil
}

// DetachCompTeam removes a publication or responsible group from a compliance
// object.
func (oDb *DB) DetachCompTeam(ctx context.Context, k CompKind, objID int64, gtype string, groupID int64) error {
	table, err := k.teamTable(gtype)
	if err != nil {
		return err
	}
	if _, err := oDb.DB.ExecContext(ctx, "DELETE FROM "+table+" WHERE "+k.FK+" = ? AND group_id = ?", objID, groupID); err != nil {
		return fmt.Errorf("detachCompTeam: %w", err)
	}
	oDb.SetChange(table)
	return nil
}

// CompObjectName returns the name of a compliance object.
func (oDb *DB) CompObjectName(ctx context.Context, k CompKind, objID int64) (string, error) {
	var name string
	if err := oDb.DB.QueryRowContext(ctx, "SELECT COALESCE("+k.NameCol+", '') FROM "+k.Table+" WHERE id = ?", objID).Scan(&name); err != nil {
		return "", fmt.Errorf("compObjectName: %w", err)
	}
	return name, nil
}

// CompObjectResponsible tells whether one of the caller's organization groups is
// responsible for a compliance object, as ruleset_responsible() and
// moduleset_responsible(); always for a manager, when the object exists.
func (oDb *DB) CompObjectResponsible(ctx context.Context, k CompKind, objID int64, groups []string, isManager bool) (bool, error) {
	if isManager {
		return oDb.exists(ctx, "compObjectResponsible", "SELECT 1 FROM "+k.Table+" WHERE id = ?", objID)
	}
	cond, args := rolesCond("ag", groups, true)
	return oDb.exists(ctx, "compObjectResponsible", "SELECT 1 FROM "+k.TeamPrefix+"responsible r"+
		" JOIN auth_group ag ON ag.id = r.group_id WHERE r."+k.FK+" = ? AND "+cond, append([]any{objID}, args...)...)
}
