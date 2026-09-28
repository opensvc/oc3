package cdb

import (
	"context"
	"fmt"
)

// CompModulesetChildAttached tells whether child is a child of parent.
func (oDb *DB) CompModulesetChildAttached(ctx context.Context, parent, child int64) (bool, error) {
	return oDb.exists(ctx, "compModulesetChildAttached",
		"SELECT 1 FROM comp_moduleset_moduleset WHERE parent_modset_id = ? AND child_modset_id = ?", parent, child)
}

// CompModulesetLoop tells whether attaching child under parent would close a
// loop: child already is an ancestor of parent.
func (oDb *DB) CompModulesetLoop(ctx context.Context, child, parent int64) (bool, error) {
	return oDb.compLoop(ctx, "compModulesetLoop", "SELECT parent_modset_id, child_modset_id FROM comp_moduleset_moduleset", child, parent)
}

// AttachCompModulesetChild makes child a child of parent.
func (oDb *DB) AttachCompModulesetChild(ctx context.Context, parent, child int64) error {
	if _, err := oDb.DB.ExecContext(ctx, "INSERT INTO comp_moduleset_moduleset (parent_modset_id, child_modset_id) VALUES (?, ?)", parent, child); err != nil {
		return fmt.Errorf("attachCompModulesetChild: %w", err)
	}
	oDb.SetChange("comp_moduleset_moduleset")
	return nil
}

// DetachCompModulesetChild removes child from the children of parent.
func (oDb *DB) DetachCompModulesetChild(ctx context.Context, parent, child int64) error {
	if _, err := oDb.DB.ExecContext(ctx, "DELETE FROM comp_moduleset_moduleset WHERE parent_modset_id = ? AND child_modset_id = ?", parent, child); err != nil {
		return fmt.Errorf("detachCompModulesetChild: %w", err)
	}
	oDb.SetChange("comp_moduleset_moduleset")
	return nil
}

// CompModulesetRulesetAttached tells whether a ruleset is attached to a moduleset.
func (oDb *DB) CompModulesetRulesetAttached(ctx context.Context, modsetID, rulesetID int64) (bool, error) {
	return oDb.exists(ctx, "compModulesetRulesetAttached",
		"SELECT 1 FROM comp_moduleset_ruleset WHERE modset_id = ? AND ruleset_id = ?", modsetID, rulesetID)
}

// AttachCompModulesetRuleset attaches a ruleset to a moduleset.
func (oDb *DB) AttachCompModulesetRuleset(ctx context.Context, modsetID, rulesetID int64) error {
	if _, err := oDb.DB.ExecContext(ctx, "INSERT INTO comp_moduleset_ruleset (modset_id, ruleset_id) VALUES (?, ?)", modsetID, rulesetID); err != nil {
		return fmt.Errorf("attachCompModulesetRuleset: %w", err)
	}
	oDb.SetChange("comp_moduleset_ruleset")
	return nil
}

// DetachCompModulesetRuleset detaches a ruleset from a moduleset.
func (oDb *DB) DetachCompModulesetRuleset(ctx context.Context, modsetID, rulesetID int64) error {
	if _, err := oDb.DB.ExecContext(ctx, "DELETE FROM comp_moduleset_ruleset WHERE modset_id = ? AND ruleset_id = ?", modsetID, rulesetID); err != nil {
		return fmt.Errorf("detachCompModulesetRuleset: %w", err)
	}
	oDb.SetChange("comp_moduleset_ruleset")
	return nil
}
