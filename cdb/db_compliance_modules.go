package cdb

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// GetComplianceModulesetModules lists the modules of a moduleset, by name; one
// when modID is set.
func (oDb *DB) GetComplianceModulesetModules(ctx context.Context, modsetID int64, modID *int64, p ListParams) ([]map[string]any, error) {
	conds := []string{"comp_moduleset_modules.modset_id = ?"}
	args := []any{modsetID}
	if modID != nil {
		conds = append(conds, "comp_moduleset_modules.id = ?")
		args = append(args, *modID)
	}
	return oDb.listQuery(ctx, "getComplianceModulesetModules", "comp_moduleset_modules", conds, args, "comp_moduleset_modules.modset_mod_name", p)
}

// CompModulesetModuleID resolves a module of a moduleset given by id or by name,
// as comp_moduleset_module_id(); false when the moduleset has none such.
func (oDb *DB) CompModulesetModuleID(ctx context.Context, modsetID int64, idOrName string) (int64, bool, error) {
	var id int64
	var err error
	if n, convErr := strconv.ParseInt(idOrName, 10, 64); convErr == nil {
		err = oDb.DB.QueryRowContext(ctx, "SELECT id FROM comp_moduleset_modules WHERE modset_id = ? AND id = ?", modsetID, n).Scan(&id)
	} else {
		err = oDb.DB.QueryRowContext(ctx, "SELECT id FROM comp_moduleset_modules WHERE modset_id = ? AND modset_mod_name = ? LIMIT 1", modsetID, idOrName).Scan(&id)
	}
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, false, nil
	case err != nil:
		return 0, false, fmt.Errorf("compModulesetModuleID: %w", err)
	}
	return id, true, nil
}

// CompModulesetModuleName returns the name of a module.
func (oDb *DB) CompModulesetModuleName(ctx context.Context, id int64) (string, error) {
	var name string
	if err := oDb.DB.QueryRowContext(ctx, "SELECT modset_mod_name FROM comp_moduleset_modules WHERE id = ?", id).Scan(&name); err != nil {
		return "", fmt.Errorf("compModulesetModuleName: %w", err)
	}
	return name, nil
}

// CreateCompModulesetModule inserts a module in a moduleset; fields holds
// modset_mod_name and optionally autofix.
func (oDb *DB) CreateCompModulesetModule(ctx context.Context, modsetID int64, fields map[string]any, author string) (int64, error) {
	cols := []string{"modset_id", "modset_mod_author", "modset_mod_updated"}
	vals := []string{"?", "?", "NOW()"}
	args := []any{modsetID, author}
	for _, k := range sortedKeys(fields) {
		cols = append(cols, k)
		vals = append(vals, "?")
		args = append(args, fields[k])
	}
	res, err := oDb.DB.ExecContext(ctx, "INSERT INTO comp_moduleset_modules ("+strings.Join(cols, ", ")+") VALUES ("+strings.Join(vals, ", ")+")", args...)
	if err != nil {
		return 0, fmt.Errorf("createCompModulesetModule: %w", err)
	}
	oDb.SetChange("comp_moduleset_modules")
	return res.LastInsertId()
}

// UpdateCompModulesetModule sets the given columns of a module, its author and
// its update date.
func (oDb *DB) UpdateCompModulesetModule(ctx context.Context, id int64, fields map[string]any, author string) error {
	sets := []string{"modset_mod_author = ?", "modset_mod_updated = NOW()"}
	args := []any{author}
	for _, k := range sortedKeys(fields) {
		sets = append(sets, k+" = ?")
		args = append(args, fields[k])
	}
	args = append(args, id)
	if _, err := oDb.DB.ExecContext(ctx, "UPDATE comp_moduleset_modules SET "+strings.Join(sets, ", ")+" WHERE id = ?", args...); err != nil {
		return fmt.Errorf("updateCompModulesetModule: %w", err)
	}
	oDb.SetChange("comp_moduleset_modules")
	return nil
}

// DeleteCompModulesetModule deletes a module.
func (oDb *DB) DeleteCompModulesetModule(ctx context.Context, id int64) error {
	if _, err := oDb.DB.ExecContext(ctx, "DELETE FROM comp_moduleset_modules WHERE id = ?", id); err != nil {
		return fmt.Errorf("deleteCompModulesetModule: %w", err)
	}
	oDb.SetChange("comp_moduleset_modules")
	return nil
}
