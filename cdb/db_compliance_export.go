package cdb

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
)

type (
	// CompExportVariable is a ruleset variable, as _export_rulesets() writes it.
	CompExportVariable struct {
		ID         int64  `json:"id"`
		VarName    string `json:"var_name"`
		VarValue   string `json:"var_value"`
		VarClass   string `json:"var_class"`
		VarAuthor  string `json:"var_author"`
		VarUpdated string `json:"var_updated"`
	}

	// CompExportRuleset is a ruleset with its variables, children, filterset and
	// groups, by name.
	CompExportRuleset struct {
		ID            int64                `json:"id"`
		RulesetName   string               `json:"ruleset_name"`
		RulesetPublic string               `json:"ruleset_public"`
		RulesetType   string               `json:"ruleset_type"`
		FsetName      *string              `json:"fset_name"`
		Variables     []CompExportVariable `json:"variables"`
		Rulesets      []string             `json:"rulesets"`
		Publications  []string             `json:"publications"`
		Responsibles  []string             `json:"responsibles"`
	}

	// CompExportModule is a moduleset module.
	CompExportModule struct {
		ModsetModName string `json:"modset_mod_name"`
		Autofix       string `json:"autofix"`
	}

	// CompExportModuleset is a moduleset with its modules, children, rulesets and
	// groups, by name.
	CompExportModuleset struct {
		ID           int64              `json:"id"`
		ModsetName   string             `json:"modset_name"`
		Modules      []CompExportModule `json:"modules"`
		Modulesets   []string           `json:"modulesets"`
		Rulesets     []string           `json:"rulesets"`
		Publications []string           `json:"publications"`
		Responsibles []string           `json:"responsibles"`
	}

	// CompRulesetExport is the export of rulesets: the rulesets and their
	// descendants, and the filtersets they use.
	CompRulesetExport struct {
		Filtersets []FiltersetExport   `json:"filtersets"`
		Rulesets   []CompExportRuleset `json:"rulesets"`
	}

	// CompModulesetExport is the export of modulesets: the modulesets and their
	// descendants, with the rulesets they hold.
	CompModulesetExport struct {
		CompRulesetExport
		Modulesets []CompExportModuleset `json:"modulesets"`
	}
)

// CompPublishedIDs returns the ids of the compliance objects published to the
// caller, as the historical export handlers select them.
func (oDb *DB) CompPublishedIDs(ctx context.Context, k CompKind, groups []string, isManager bool) ([]int64, error) {
	cond, args := compObjectVisibleCond(k, groups, isManager)
	return oDb.int64s(ctx, "compPublishedIDs", "SELECT "+k.Table+".id FROM "+k.Table+" WHERE "+cond+" ORDER BY "+k.Table+".id", args...)
}

func (oDb *DB) int64s(ctx context.Context, name, query string, args ...any) ([]int64, error) {
	rows, err := oDb.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	defer func() { _ = rows.Close() }()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// compPairs reads parent-child id pairs.
func (oDb *DB) compPairs(ctx context.Context, name, query string) (map[int64][]int64, error) {
	rows, err := oDb.DB.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	defer func() { _ = rows.Close() }()
	pairs := map[int64][]int64{}
	for rows.Next() {
		var p, c int64
		if err := rows.Scan(&p, &c); err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		pairs[p] = append(pairs[p], c)
	}
	return pairs, rows.Err()
}

// withDescendants returns the ids and all the children reachable from them.
func withDescendants(ids []int64, children map[int64][]int64) []int64 {
	seen := map[int64]bool{}
	todo := append([]int64{}, ids...)
	var all []int64
	for len(todo) > 0 {
		id := todo[0]
		todo = todo[1:]
		if seen[id] {
			continue
		}
		seen[id] = true
		all = append(all, id)
		todo = append(todo, children[id]...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })
	return all
}

func idArgs(ids []int64) (string, []any) {
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	return Placeholders(len(ids)), args
}

// compNames returns the names of the objects of a table, by id.
func (oDb *DB) compNames(ctx context.Context, table, nameCol string) (map[int64]string, error) {
	rows, err := oDb.DB.QueryContext(ctx, "SELECT id, COALESCE("+nameCol+", '') FROM "+table)
	if err != nil {
		return nil, fmt.Errorf("compNames %s: %w", table, err)
	}
	defer func() { _ = rows.Close() }()
	names := map[int64]string{}
	for rows.Next() {
		var id int64
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			return nil, fmt.Errorf("compNames %s: %w", table, err)
		}
		names[id] = name
	}
	return names, rows.Err()
}

// compTeamRoles returns the roles of the publication or responsible groups of
// the given objects, by object id.
func (oDb *DB) compTeamRoles(ctx context.Context, k CompKind, gtype string, ids []int64) (map[int64][]string, error) {
	table, err := k.teamTable(gtype)
	if err != nil {
		return nil, err
	}
	ph, args := idArgs(ids)
	rows, err := oDb.DB.QueryContext(ctx, "SELECT t."+k.FK+", ag.role FROM "+table+" t JOIN auth_group ag ON ag.id = t.group_id"+
		" WHERE t."+k.FK+" IN ("+ph+") ORDER BY ag.role", args...)
	if err != nil {
		return nil, fmt.Errorf("compTeamRoles: %w", err)
	}
	defer func() { _ = rows.Close() }()
	roles := map[int64][]string{}
	for rows.Next() {
		var id int64
		var role string
		if err := rows.Scan(&id, &role); err != nil {
			return nil, fmt.Errorf("compTeamRoles: %w", err)
		}
		roles[id] = append(roles[id], role)
	}
	return roles, rows.Err()
}

func mapNames(ids []int64, names map[int64]string) []string {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		out = append(out, names[id])
	}
	sort.Strings(out)
	return out
}

func orEmpty(l []string) []string {
	if l == nil {
		return []string{}
	}
	return l
}

// ExportCompRulesets exports rulesets and their descendants, with the
// filtersets they use, in the format ImportCompliance reads, as
// _export_rulesets().
func (oDb *DB) ExportCompRulesets(ctx context.Context, ids []int64) (CompRulesetExport, error) {
	out := CompRulesetExport{Filtersets: []FiltersetExport{}, Rulesets: []CompExportRuleset{}}
	if len(ids) == 0 {
		return out, nil
	}
	children, err := oDb.compPairs(ctx, "exportCompRulesets", "SELECT parent_rset_id, child_rset_id FROM comp_rulesets_rulesets")
	if err != nil {
		return out, err
	}
	names, err := oDb.compNames(ctx, "comp_rulesets", "ruleset_name")
	if err != nil {
		return out, err
	}
	all := withDescendants(ids, children)
	ph, args := idArgs(all)

	variables := map[int64][]CompExportVariable{}
	rows, err := oDb.DB.QueryContext(ctx, "SELECT id, ruleset_id, COALESCE(var_name, ''), COALESCE(var_value, ''), COALESCE(var_class, ''),"+
		" COALESCE(var_author, ''), COALESCE(DATE_FORMAT(var_updated, '%Y-%m-%d %H:%i:%s'), '')"+
		" FROM comp_rulesets_variables WHERE ruleset_id IN ("+ph+") ORDER BY var_name", args...)
	if err != nil {
		return out, fmt.Errorf("exportCompRulesets variables: %w", err)
	}
	for rows.Next() {
		var v CompExportVariable
		var rid int64
		if err := rows.Scan(&v.ID, &rid, &v.VarName, &v.VarValue, &v.VarClass, &v.VarAuthor, &v.VarUpdated); err != nil {
			_ = rows.Close()
			return out, fmt.Errorf("exportCompRulesets variables: %w", err)
		}
		variables[rid] = append(variables[rid], v)
	}
	_ = rows.Close()

	publications, err := oDb.compTeamRoles(ctx, CompRulesetKind, "publication", all)
	if err != nil {
		return out, err
	}
	responsibles, err := oDb.compTeamRoles(ctx, CompRulesetKind, "responsible", all)
	if err != nil {
		return out, err
	}

	var fsetIDs []int
	rows, err = oDb.DB.QueryContext(ctx, "SELECT r.id, COALESCE(r.ruleset_name, ''), COALESCE(r.ruleset_public, ''), COALESCE(r.ruleset_type, ''), f.id, f.fset_name"+
		" FROM comp_rulesets r LEFT JOIN comp_rulesets_filtersets rf ON rf.ruleset_id = r.id LEFT JOIN gen_filtersets f ON f.id = rf.fset_id"+
		" WHERE r.id IN ("+ph+") ORDER BY r.ruleset_name", args...)
	if err != nil {
		return out, fmt.Errorf("exportCompRulesets: %w", err)
	}
	for rows.Next() {
		var r CompExportRuleset
		var fsetID sql.NullInt64
		var fsetName sql.NullString
		if err := rows.Scan(&r.ID, &r.RulesetName, &r.RulesetPublic, &r.RulesetType, &fsetID, &fsetName); err != nil {
			_ = rows.Close()
			return out, fmt.Errorf("exportCompRulesets: %w", err)
		}
		if fsetID.Valid {
			fsetIDs = append(fsetIDs, int(fsetID.Int64))
			name := fsetName.String
			r.FsetName = &name
		}
		r.Variables = variables[r.ID]
		if r.Variables == nil {
			r.Variables = []CompExportVariable{}
		}
		r.Rulesets = mapNames(children[r.ID], names)
		r.Publications = orEmpty(publications[r.ID])
		r.Responsibles = orEmpty(responsibles[r.ID])
		out.Rulesets = append(out.Rulesets, r)
	}
	_ = rows.Close()

	fsets, err := oDb.ExportFiltersets(ctx, fsetIDs)
	if err != nil {
		return out, err
	}
	out.Filtersets = fsets.Filtersets
	return out, nil
}

// ExportCompModulesets exports modulesets and their descendants, with the
// rulesets they hold, in the format ImportCompliance reads, as
// _export_modulesets().
func (oDb *DB) ExportCompModulesets(ctx context.Context, ids []int64) (CompModulesetExport, error) {
	out := CompModulesetExport{Modulesets: []CompExportModuleset{}}
	out.Filtersets = []FiltersetExport{}
	out.Rulesets = []CompExportRuleset{}
	if len(ids) == 0 {
		return out, nil
	}
	children, err := oDb.compPairs(ctx, "exportCompModulesets", "SELECT parent_modset_id, child_modset_id FROM comp_moduleset_moduleset")
	if err != nil {
		return out, err
	}
	rulesets, err := oDb.compPairs(ctx, "exportCompModulesets", "SELECT modset_id, ruleset_id FROM comp_moduleset_ruleset")
	if err != nil {
		return out, err
	}
	names, err := oDb.compNames(ctx, "comp_moduleset", "modset_name")
	if err != nil {
		return out, err
	}
	rsetNames, err := oDb.compNames(ctx, "comp_rulesets", "ruleset_name")
	if err != nil {
		return out, err
	}
	all := withDescendants(ids, children)
	ph, args := idArgs(all)

	modules := map[int64][]CompExportModule{}
	rows, err := oDb.DB.QueryContext(ctx, "SELECT modset_id, COALESCE(modset_mod_name, ''), COALESCE(autofix, '')"+
		" FROM comp_moduleset_modules WHERE modset_id IN ("+ph+") ORDER BY modset_mod_name", args...)
	if err != nil {
		return out, fmt.Errorf("exportCompModulesets modules: %w", err)
	}
	for rows.Next() {
		var m CompExportModule
		var mid int64
		if err := rows.Scan(&mid, &m.ModsetModName, &m.Autofix); err != nil {
			_ = rows.Close()
			return out, fmt.Errorf("exportCompModulesets modules: %w", err)
		}
		modules[mid] = append(modules[mid], m)
	}
	_ = rows.Close()

	publications, err := oDb.compTeamRoles(ctx, CompModulesetKind, "publication", all)
	if err != nil {
		return out, err
	}
	responsibles, err := oDb.compTeamRoles(ctx, CompModulesetKind, "responsible", all)
	if err != nil {
		return out, err
	}

	var rsetIDs []int64
	for _, id := range all {
		m := CompExportModuleset{
			ID:           id,
			ModsetName:   names[id],
			Modules:      modules[id],
			Modulesets:   mapNames(children[id], names),
			Rulesets:     mapNames(rulesets[id], rsetNames),
			Publications: orEmpty(publications[id]),
			Responsibles: orEmpty(responsibles[id]),
		}
		if m.Modules == nil {
			m.Modules = []CompExportModule{}
		}
		rsetIDs = append(rsetIDs, rulesets[id]...)
		out.Modulesets = append(out.Modulesets, m)
	}
	sort.Slice(out.Modulesets, func(i, j int) bool {
		return strings.ToLower(out.Modulesets[i].ModsetName) < strings.ToLower(out.Modulesets[j].ModsetName)
	})

	rsets, err := oDb.ExportCompRulesets(ctx, rsetIDs)
	if err != nil {
		return out, err
	}
	out.CompRulesetExport = rsets
	return out, nil
}
