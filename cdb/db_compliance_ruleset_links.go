package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// CompRulesetChildAttached tells whether child is a child of parent.
func (oDb *DB) CompRulesetChildAttached(ctx context.Context, parent, child int64) (bool, error) {
	return oDb.exists(ctx, "compRulesetChildAttached",
		"SELECT 1 FROM comp_rulesets_rulesets WHERE parent_rset_id = ? AND child_rset_id = ?", parent, child)
}

// CompRulesetLoop tells whether attaching child under parent would close a loop:
// child already is an ancestor of parent, as rset_loop().
func (oDb *DB) CompRulesetLoop(ctx context.Context, child, parent int64) (bool, error) {
	return oDb.compLoop(ctx, "compRulesetLoop", "SELECT parent_rset_id, child_rset_id FROM comp_rulesets_rulesets", child, parent)
}

// compLoop tells whether child is an ancestor of parent in the parent-child
// pairs the query returns.
func (oDb *DB) compLoop(ctx context.Context, name, query string, child, parent int64) (bool, error) {
	parents := map[int64][]int64{}
	rows, err := oDb.DB.QueryContext(ctx, query)
	if err != nil {
		return false, fmt.Errorf("%s: %w", name, err)
	}
	for rows.Next() {
		var p, c int64
		if err := rows.Scan(&p, &c); err != nil {
			_ = rows.Close()
			return false, fmt.Errorf("%s: %w", name, err)
		}
		parents[c] = append(parents[c], p)
	}
	_ = rows.Close()
	seen := map[int64]bool{}
	todo := []int64{parent}
	for len(todo) > 0 {
		id := todo[0]
		todo = todo[1:]
		for _, p := range parents[id] {
			if p == child {
				return true, nil
			}
			if !seen[p] {
				seen[p] = true
				todo = append(todo, p)
			}
		}
	}
	return false, nil
}

// AttachCompRulesetChild makes child a child of parent.
func (oDb *DB) AttachCompRulesetChild(ctx context.Context, parent, child int64) error {
	if _, err := oDb.DB.ExecContext(ctx, "INSERT INTO comp_rulesets_rulesets (parent_rset_id, child_rset_id) VALUES (?, ?)", parent, child); err != nil {
		return fmt.Errorf("attachCompRulesetChild: %w", err)
	}
	oDb.SetChange("comp_rulesets_rulesets")
	return nil
}

// DetachCompRulesetChild removes child from the children of parent.
func (oDb *DB) DetachCompRulesetChild(ctx context.Context, parent, child int64) error {
	if _, err := oDb.DB.ExecContext(ctx, "DELETE FROM comp_rulesets_rulesets WHERE parent_rset_id = ? AND child_rset_id = ?", parent, child); err != nil {
		return fmt.Errorf("detachCompRulesetChild: %w", err)
	}
	oDb.SetChange("comp_rulesets_rulesets")
	return nil
}

// CompRulesetFilterset returns the filterset of a contextual ruleset, with its
// name; false when it has none.
func (oDb *DB) CompRulesetFilterset(ctx context.Context, rulesetID int64) (int64, string, bool, error) {
	var id int64
	var name sql.NullString
	err := oDb.DB.QueryRowContext(ctx, "SELECT f.id, f.fset_name FROM comp_rulesets_filtersets rf"+
		" JOIN gen_filtersets f ON f.id = rf.fset_id WHERE rf.ruleset_id = ? LIMIT 1", rulesetID).Scan(&id, &name)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, "", false, nil
	case err != nil:
		return 0, "", false, fmt.Errorf("compRulesetFilterset: %w", err)
	}
	return id, name.String, true, nil
}

// SetCompRulesetFilterset gives a ruleset its filterset, replacing the previous
// one: a ruleset has at most one, as update_or_insert() keeps it.
func (oDb *DB) SetCompRulesetFilterset(ctx context.Context, rulesetID, fsetID int64) error {
	if _, err := oDb.DB.ExecContext(ctx, "DELETE FROM comp_rulesets_filtersets WHERE ruleset_id = ?", rulesetID); err != nil {
		return fmt.Errorf("setCompRulesetFilterset: %w", err)
	}
	if _, err := oDb.DB.ExecContext(ctx, "INSERT INTO comp_rulesets_filtersets (ruleset_id, fset_id) VALUES (?, ?)", rulesetID, fsetID); err != nil {
		return fmt.Errorf("setCompRulesetFilterset: %w", err)
	}
	oDb.SetChange("comp_rulesets_filtersets")
	return nil
}

// DeleteCompRulesetFilterset removes the filterset of a ruleset.
func (oDb *DB) DeleteCompRulesetFilterset(ctx context.Context, rulesetID int64) error {
	if _, err := oDb.DB.ExecContext(ctx, "DELETE FROM comp_rulesets_filtersets WHERE ruleset_id = ?", rulesetID); err != nil {
		return fmt.Errorf("deleteCompRulesetFilterset: %w", err)
	}
	oDb.SetChange("comp_rulesets_filtersets")
	return nil
}
