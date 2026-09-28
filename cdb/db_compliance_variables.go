package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/opensvc/oc3/schema"
)

// The derived tables rulesetsVariablesFrom joins, declared as columns so the
// lists can be filtered and sorted by them: the team roles of each ruleset, and
// the chains from a ruleset to itself and to each ruleset it encapsulates.
var (
	TRsetResponsibles = &schema.Table{Name: "rset_responsibles"}
	TRsetPublications = &schema.Table{Name: "rset_publications"}
	TRsetChains       = &schema.Table{Name: "rset_chains"}
	RsetResponsibles  = &schema.Col{T: TRsetResponsibles, Name: "teams", Nullable: true}
	RsetPublications  = &schema.Col{T: TRsetPublications, Name: "teams", Nullable: true}
	RsetChainsChain   = &schema.Col{T: TRsetChains, Name: "chain", Nullable: true}
	RsetChainsLen     = &schema.Col{T: TRsetChains, Name: "chain_len", Nullable: true}
	RsetChainsEncap   = &schema.Col{T: TRsetChains, Name: "encap_rset", Nullable: true}
	RsetChainsEncapID = &schema.Col{T: TRsetChains, Name: "encap_rset_id", Nullable: true}
)

// rulesetsVariablesFrom is the historical v_comp_rulesets view: every ruleset
// with its filterset and teams, and the variables of the ruleset itself then of
// each ruleset it encapsulates, found through comp_rulesets_chains. On its own
// chain a ruleset names no encapsulated ruleset; a ruleset of the chain without
// variable is on a row of its own.
var rulesetsVariablesFrom = "comp_rulesets" +
	" LEFT JOIN comp_rulesets_filtersets ON comp_rulesets_filtersets.ruleset_id = comp_rulesets.id" +
	" LEFT JOIN gen_filtersets ON gen_filtersets.id = comp_rulesets_filtersets.fset_id " +
	compTeamsJoin(CompRulesetKind, "responsible", TRsetResponsibles.Name) + " " +
	compTeamsJoin(CompRulesetKind, "publication", TRsetPublications.Name) +
	" LEFT JOIN (SELECT c.head_rset_id, c.tail_rset_id, c.chain, c.chain_len," +
	" IF(c.tail_rset_id = c.head_rset_id, '', r.ruleset_name) AS encap_rset," +
	" IF(c.tail_rset_id = c.head_rset_id, NULL, c.tail_rset_id) AS encap_rset_id" +
	" FROM comp_rulesets_chains c JOIN comp_rulesets r ON r.id = c.tail_rset_id) " + TRsetChains.Name +
	" ON " + TRsetChains.Name + ".head_rset_id = comp_rulesets.id" +
	" LEFT JOIN comp_rulesets_variables ON comp_rulesets_variables.ruleset_id = " + TRsetChains.Name + ".tail_rset_id"

// GetComplianceRulesetsVariables lists the variables of the rulesets visible to
// the caller, encapsulated ones included, by ruleset, chain and variable name.
func (oDb *DB) GetComplianceRulesetsVariables(ctx context.Context, p ListParams) ([]map[string]any, error) {
	cond, args := compRulesetVisibleCond(p.Groups, p.IsManager)
	return oDb.listQuery(ctx, "getComplianceRulesetsVariables", rulesetsVariablesFrom, []string{cond}, args,
		"comp_rulesets.ruleset_name, "+RsetChainsLen.Qualified()+", "+RsetChainsEncap.Qualified()+", comp_rulesets_variables.var_name", p)
}

// CompRulesetPublished tells whether a ruleset is published to the caller: to
// one of the caller's groups or to Everybody, as ruleset_publication(); always
// for a manager.
func (oDb *DB) CompRulesetPublished(ctx context.Context, id int64, groups []string, isManager bool) (bool, error) {
	if ok, err := oDb.CompRulesetVisible(ctx, id, groups, isManager); err != nil || ok {
		return ok, err
	}
	return oDb.RulesetHasEverybodyPublication(ctx, strconv.FormatInt(id, 10))
}

// GetComplianceRulesetVariables lists the variables of a ruleset, by name; one
// when varID is set.
func (oDb *DB) GetComplianceRulesetVariables(ctx context.Context, rulesetID int64, varID *int64, p ListParams) ([]map[string]any, error) {
	conds := []string{"comp_rulesets_variables.ruleset_id = ?"}
	args := []any{rulesetID}
	if varID != nil {
		conds = append(conds, "comp_rulesets_variables.id = ?")
		args = append(args, *varID)
	}
	return oDb.listQuery(ctx, "getComplianceRulesetVariables", "comp_rulesets_variables", conds, args, "comp_rulesets_variables.var_name", p)
}

// CompRulesetVariableID resolves a variable of a ruleset given by id or by name,
// as comp_ruleset_variable_id(); false when the ruleset has none such.
func (oDb *DB) CompRulesetVariableID(ctx context.Context, rulesetID int64, idOrName string) (int64, bool, error) {
	var id int64
	var err error
	if n, convErr := strconv.ParseInt(idOrName, 10, 64); convErr == nil {
		err = oDb.DB.QueryRowContext(ctx, "SELECT id FROM comp_rulesets_variables WHERE ruleset_id = ? AND id = ?", rulesetID, n).Scan(&id)
	} else {
		err = oDb.DB.QueryRowContext(ctx, "SELECT id FROM comp_rulesets_variables WHERE ruleset_id = ? AND var_name = ? LIMIT 1", rulesetID, idOrName).Scan(&id)
	}
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, false, nil
	case err != nil:
		return 0, false, fmt.Errorf("compRulesetVariableID: %w", err)
	}
	return id, true, nil
}

// CompRulesetVariable is a variable of a ruleset, as the change logs name it.
type CompRulesetVariable struct {
	ID          int64
	RulesetID   int64
	RulesetName string
	Name        string
	Class       string
	Value       string
}

// CompRulesetVariableByID returns a variable with the name of its ruleset.
func (oDb *DB) CompRulesetVariableByID(ctx context.Context, id int64) (*CompRulesetVariable, error) {
	var v CompRulesetVariable
	var rname, value sql.NullString
	err := oDb.DB.QueryRowContext(ctx, "SELECT v.id, v.ruleset_id, r.ruleset_name, v.var_name, v.var_class, v.var_value"+
		" FROM comp_rulesets_variables v JOIN comp_rulesets r ON r.id = v.ruleset_id WHERE v.id = ?", id).
		Scan(&v.ID, &v.RulesetID, &rname, &v.Name, &v.Class, &value)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, ErrCompNotFound
	case err != nil:
		return nil, fmt.Errorf("compRulesetVariableByID: %w", err)
	}
	v.RulesetName, v.Value = rname.String, value.String
	return &v, nil
}

// CreateCompRulesetVariable inserts a variable in a ruleset; fields holds
// var_name and optionally var_class and var_value.
func (oDb *DB) CreateCompRulesetVariable(ctx context.Context, rulesetID int64, fields map[string]any, author string) (int64, error) {
	cols := []string{"ruleset_id", "var_author", "var_updated"}
	vals := []string{"?", "?", "NOW()"}
	args := []any{rulesetID, author}
	if _, ok := fields["var_class"]; !ok {
		fields["var_class"] = ""
	}
	for _, k := range sortedKeys(fields) {
		cols = append(cols, k)
		vals = append(vals, "?")
		args = append(args, fields[k])
	}
	res, err := oDb.DB.ExecContext(ctx, "INSERT INTO comp_rulesets_variables ("+strings.Join(cols, ", ")+") VALUES ("+strings.Join(vals, ", ")+")", args...)
	if err != nil {
		return 0, fmt.Errorf("createCompRulesetVariable: %w", err)
	}
	oDb.SetChange("comp_rulesets_variables")
	return res.LastInsertId()
}

// UpdateCompRulesetVariable sets the given columns of a variable, its author and
// its update date.
func (oDb *DB) UpdateCompRulesetVariable(ctx context.Context, id int64, fields map[string]any, author string) error {
	sets := []string{"var_author = ?", "var_updated = NOW()"}
	args := []any{author}
	for _, k := range sortedKeys(fields) {
		sets = append(sets, k+" = ?")
		args = append(args, fields[k])
	}
	args = append(args, id)
	if _, err := oDb.DB.ExecContext(ctx, "UPDATE comp_rulesets_variables SET "+strings.Join(sets, ", ")+" WHERE id = ?", args...); err != nil {
		return fmt.Errorf("updateCompRulesetVariable: %w", err)
	}
	oDb.SetChange("comp_rulesets_variables")
	return nil
}

// DeleteCompRulesetVariable deletes a variable.
func (oDb *DB) DeleteCompRulesetVariable(ctx context.Context, id int64) error {
	if _, err := oDb.DB.ExecContext(ctx, "DELETE FROM comp_rulesets_variables WHERE id = ?", id); err != nil {
		return fmt.Errorf("deleteCompRulesetVariable: %w", err)
	}
	oDb.SetChange("comp_rulesets_variables")
	return nil
}

// CopyCompRulesetVariable copies a variable to another ruleset, authored by
// author, as copy_variable_to_ruleset().
func (oDb *DB) CopyCompRulesetVariable(ctx context.Context, id, dstRulesetID int64, author string) (int64, error) {
	res, err := oDb.DB.ExecContext(ctx, "INSERT INTO comp_rulesets_variables (ruleset_id, var_name, var_class, var_value, var_author, var_updated)"+
		" SELECT ?, var_name, var_class, var_value, ?, NOW() FROM comp_rulesets_variables WHERE id = ?", dstRulesetID, author, id)
	if err != nil {
		return 0, fmt.Errorf("copyCompRulesetVariable: %w", err)
	}
	oDb.SetChange("comp_rulesets_variables")
	return res.LastInsertId()
}

// MoveCompRulesetVariable moves a variable to another ruleset, as
// move_variable_to_ruleset().
func (oDb *DB) MoveCompRulesetVariable(ctx context.Context, id, dstRulesetID int64) error {
	if _, err := oDb.DB.ExecContext(ctx, "UPDATE comp_rulesets_variables SET ruleset_id = ? WHERE id = ?", dstRulesetID, id); err != nil {
		return fmt.Errorf("moveCompRulesetVariable: %w", err)
	}
	oDb.SetChange("comp_rulesets_variables")
	return nil
}
