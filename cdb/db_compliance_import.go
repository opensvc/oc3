package cdb

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var (
	// ErrCompInvalid marks compliance designer data that cannot be used.
	ErrCompInvalid = errors.New("invalid data")

	// ErrCompForbidden marks a compliance designer change the caller is not
	// allowed to make.
	ErrCompForbidden = errors.New("forbidden")
)

// CompFlag is a boolean of the compliance designer, stored as "T" or "F". It
// reads a JSON boolean, number or string, as the historical exports hold either.
type CompFlag string

func (f *CompFlag) UnmarshalJSON(b []byte) error {
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch t := v.(type) {
	case bool:
		*f = "F"
		if t {
			*f = "T"
		}
		return nil
	case float64:
		if t == 0 || t == 1 {
			*f = CompFlag(map[bool]string{true: "T", false: "F"}[t == 1])
			return nil
		}
	case string:
		switch strings.ToLower(strings.TrimSpace(t)) {
		case "t", "true", "yes", "y", "1":
			*f = "T"
			return nil
		case "f", "false", "no", "n", "0", "":
			*f = "F"
			return nil
		}
	}
	return fmt.Errorf("invalid boolean %s", b)
}

type (
	// CompImportFilter is a filter of an imported filterset.
	CompImportFilter struct {
		FTable *string `json:"f_table"`
		FField *string `json:"f_field"`
		FOp    *string `json:"f_op"`
		FValue *string `json:"f_value"`
	}

	// CompImportFilterEntry is a filter or a filterset in an imported filterset.
	CompImportFilterEntry struct {
		FLogOp    string            `json:"f_log_op"`
		FOrder    int               `json:"f_order"`
		Filter    *CompImportFilter `json:"filter"`
		Filterset *string           `json:"filterset"`
	}

	// CompImportFilterset is an imported filterset.
	CompImportFilterset struct {
		FsetName *string                 `json:"fset_name"`
		Filters  []CompImportFilterEntry `json:"filters"`
	}

	// CompImportVariable is a variable of an imported ruleset.
	CompImportVariable struct {
		VarName  *string `json:"var_name"`
		VarClass *string `json:"var_class"`
		VarValue *string `json:"var_value"`
	}

	// CompImportRuleset is an imported ruleset.
	CompImportRuleset struct {
		RulesetName   *string              `json:"ruleset_name"`
		RulesetType   *string              `json:"ruleset_type"`
		RulesetPublic *CompFlag            `json:"ruleset_public"`
		FsetName      *string              `json:"fset_name"`
		Variables     []CompImportVariable `json:"variables"`
		Rulesets      []string             `json:"rulesets"`
	}

	// CompImportModule is a module of an imported moduleset.
	CompImportModule struct {
		ModsetModName *string   `json:"modset_mod_name"`
		Autofix       *CompFlag `json:"autofix"`
	}

	// CompImportModuleset is an imported moduleset.
	CompImportModuleset struct {
		ModsetName *string            `json:"modset_name"`
		Modules    []CompImportModule `json:"modules"`
		Modulesets []string           `json:"modulesets"`
		Rulesets   []string           `json:"rulesets"`
	}

	// CompImportData is what ExportCompRulesets and ExportCompModulesets write.
	// The publications and responsibles of the export are not imported: the new
	// objects get the importer's default group, as add_default_teams() gives.
	CompImportData struct {
		Filtersets []CompImportFilterset `json:"filtersets"`
		Rulesets   []CompImportRuleset   `json:"rulesets"`
		Modulesets []CompImportModuleset `json:"modulesets"`
	}

	// CompImporter is who imports: the author recorded on the new objects, the
	// group they are given, and whether the caller is responsible for an existing
	// object the import adds to.
	CompImporter struct {
		Author      string
		GroupID     int64
		Responsible func(ctx context.Context, k CompKind, id int64) (bool, error)
	}

	compImport struct {
		oDb      *DB
		imp      CompImporter
		messages []string
		filters  map[string]int
		fsets    map[string]int
		rsets    map[string]int64
		modsets  map[string]int64
		// existing holds the objects the import found already there, which it
		// may add to only when the caller is responsible for them.
		existing map[string]bool
	}
)

func invalidf(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrCompInvalid}, args...)...)
}

// ImportCompliance creates the filters, filtersets, rulesets and modulesets of
// an export, and their relations, as lib_compliance_import(): an object already
// there by name is reused, and the messages tell what was added and what already
// existed. Unlike the historical import, content is added to an existing
// ruleset or moduleset only when the caller is responsible for it, and a
// relation closing a loop is refused. Run it in a transaction, as it stops at
// the first error.
func (oDb *DB) ImportCompliance(ctx context.Context, data CompImportData, imp CompImporter) ([]string, error) {
	ci := &compImport{
		oDb:      oDb,
		imp:      imp,
		messages: []string{},
		filters:  map[string]int{},
		fsets:    map[string]int{},
		rsets:    map[string]int64{},
		modsets:  map[string]int64{},
		existing: map[string]bool{},
	}
	for _, step := range []func(context.Context, CompImportData) error{
		ci.importFilters, ci.importFiltersets, ci.importFiltersetRelations,
		ci.importRulesets, ci.importRulesetFiltersets, ci.importRulesetRelations, ci.importVariables,
		ci.importModulesets, ci.importModules, ci.importModulesetRelations, ci.importModulesetRulesets,
	} {
		if err := step(ctx, data); err != nil {
			return nil, err
		}
	}
	return ci.messages, nil
}

func (ci *compImport) add(format string, args ...any) {
	ci.messages = append(ci.messages, fmt.Sprintf(format, args...))
}

// mayChange refuses an addition to an existing object the caller is not
// responsible for.
func (ci *compImport) mayChange(ctx context.Context, k CompKind, name string, id int64) error {
	if !ci.existing[k.Name+"/"+name] {
		return nil
	}
	ok, err := ci.imp.Responsible(ctx, k, id)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: the %s %s already exists and you are not responsible for it", ErrCompForbidden, k.Name, name)
	}
	return nil
}

func filterKey(f *CompImportFilter) (string, error) {
	if f.FTable == nil || f.FField == nil || f.FOp == nil || f.FValue == nil {
		b, _ := json.Marshal(f)
		return "", invalidf("invalid filter format: %s", b)
	}
	return *f.FTable + "." + *f.FField + " " + *f.FOp + " " + *f.FValue, nil
}

func (ci *compImport) importFilters(ctx context.Context, data CompImportData) error {
	for _, fset := range data.Filtersets {
		for _, entry := range fset.Filters {
			f := entry.Filter
			if f == nil {
				continue
			}
			key, err := filterKey(f)
			if err != nil {
				return err
			}
			if _, done := ci.filters[key]; done {
				continue
			}
			id, found, err := ci.oDb.FilterByDefinition(ctx, *f.FTable, *f.FField, *f.FOp, *f.FValue)
			if err != nil {
				return err
			}
			if found {
				ci.filters[key] = id
				ci.add("Filter already exists: %s", key)
				continue
			}
			if id, err = ci.oDb.InsertFilter(ctx, *f.FTable, *f.FField, *f.FOp, *f.FValue, ci.imp.Author); err != nil {
				return err
			}
			ci.oDb.SetChange("gen_filters")
			ci.filters[key] = id
			ci.add("Filter added: %s", key)
		}
	}
	return nil
}

func (ci *compImport) importFiltersets(ctx context.Context, data CompImportData) error {
	for _, fset := range data.Filtersets {
		if fset.FsetName == nil || *fset.FsetName == "" {
			return invalidf("invalid filterset format: a fset_name is expected")
		}
		name := *fset.FsetName
		id, found, err := ci.oDb.FiltersetByName(ctx, name)
		if err != nil {
			return err
		}
		if found {
			ci.fsets[name] = id
			ci.add("Filterset already exists: %s", name)
			continue
		}
		if id, err = ci.oDb.InsertFilterset(ctx, name, "F", ci.imp.Author); err != nil {
			return err
		}
		ci.oDb.SetChange("gen_filtersets")
		ci.fsets[name] = id
		ci.add("Filterset added: %s", name)
	}
	return nil
}

// filtersetID resolves a filterset of the import, or one already there.
func (ci *compImport) filtersetID(ctx context.Context, name string) (int, error) {
	if id, ok := ci.fsets[name]; ok {
		return id, nil
	}
	id, found, err := ci.oDb.FiltersetByName(ctx, name)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, invalidf("filterset %s not found", name)
	}
	ci.fsets[name] = id
	return id, nil
}

func (ci *compImport) importFiltersetRelations(ctx context.Context, data CompImportData) error {
	for _, fset := range data.Filtersets {
		fsetID := ci.fsets[*fset.FsetName]
		for _, entry := range fset.Filters {
			order := strconv.Itoa(entry.FOrder)
			switch {
			case entry.Filter != nil:
				key, _ := filterKey(entry.Filter)
				fID := ci.filters[key]
				rel := *fset.FsetName + " -> " + entry.FLogOp + " " + key + " (" + order + ")"
				if found, err := ci.oDb.exists(ctx, "importFiltersetRelations",
					"SELECT 1 FROM gen_filtersets_filters WHERE fset_id = ? AND f_id = ? AND f_log_op = ? AND f_order = ?",
					fsetID, fID, entry.FLogOp, entry.FOrder); err != nil {
					return err
				} else if found {
					ci.add("Filterset relation already exists: %s", rel)
					continue
				}
				if err := ci.oDb.InsertFiltersetFilter(ctx, fsetID, fID, entry.FOrder, entry.FLogOp); err != nil {
					return err
				}
				ci.add("Filterset relation added: %s", rel)
			case entry.Filterset != nil:
				encapID, err := ci.filtersetID(ctx, *entry.Filterset)
				if err != nil {
					return err
				}
				rel := *fset.FsetName + " -> " + entry.FLogOp + " " + *entry.Filterset + " (" + order + ")"
				if found, err := ci.oDb.exists(ctx, "importFiltersetRelations",
					"SELECT 1 FROM gen_filtersets_filters WHERE fset_id = ? AND encap_fset_id = ? AND f_log_op = ? AND f_order = ?",
					fsetID, encapID, entry.FLogOp, entry.FOrder); err != nil {
					return err
				} else if found {
					ci.add("Filterset relation already exists: %s", rel)
					continue
				}
				if loop, err := ci.oDb.FiltersetEncapWouldLoop(ctx, encapID, fsetID); err != nil {
					return err
				} else if loop || encapID == fsetID {
					return invalidf("the filterset relation %s would cause an infinite recursion", rel)
				}
				if err := ci.oDb.InsertFiltersetEncap(ctx, fsetID, encapID, entry.FOrder, entry.FLogOp); err != nil {
					return err
				}
				ci.add("Filterset relation added: %s", rel)
			default:
				continue
			}
			ci.oDb.SetChange("gen_filtersets_filters")
		}
	}
	return nil
}

// idByName returns the id of the object of a table named name.
func (ci *compImport) idByName(ctx context.Context, table, nameCol, name string) (int64, bool, error) {
	var id int64
	err := ci.oDb.DB.QueryRowContext(ctx, "SELECT id FROM "+table+" WHERE "+nameCol+" = ? LIMIT 1", name).Scan(&id)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, false, nil
	case err != nil:
		return 0, false, fmt.Errorf("idByName %s: %w", table, err)
	}
	return id, true, nil
}

func (ci *compImport) importRulesets(ctx context.Context, data CompImportData) error {
	for _, rset := range data.Rulesets {
		if rset.RulesetName == nil || *rset.RulesetName == "" || rset.RulesetType == nil || rset.RulesetPublic == nil {
			b, _ := json.Marshal(rset)
			return invalidf("invalid ruleset format: %s", b)
		}
		name := *rset.RulesetName
		if *rset.RulesetType != "explicit" && *rset.RulesetType != "contextual" {
			return invalidf("invalid ruleset_type %q of ruleset %s: expected explicit or contextual", *rset.RulesetType, name)
		}
		id, found, err := ci.idByName(ctx, "comp_rulesets", "ruleset_name", name)
		if err != nil {
			return err
		}
		if found {
			ci.rsets[name] = id
			ci.existing[CompRulesetKind.Name+"/"+name] = true
			ci.add("Ruleset already exists: %s", name)
			continue
		}
		if id, err = ci.oDb.CreateCompRuleset(ctx, name, *rset.RulesetType, string(*rset.RulesetPublic), ci.imp.GroupID); err != nil {
			return err
		}
		ci.rsets[name] = id
		ci.add("Ruleset added: %s", name)
	}
	return nil
}

// rulesetID resolves a ruleset of the import, or one already there.
func (ci *compImport) rulesetID(ctx context.Context, name string) (int64, error) {
	if id, ok := ci.rsets[name]; ok {
		return id, nil
	}
	id, found, err := ci.idByName(ctx, "comp_rulesets", "ruleset_name", name)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, invalidf("ruleset %s not found", name)
	}
	ci.rsets[name] = id
	return id, nil
}

func (ci *compImport) importRulesetFiltersets(ctx context.Context, data CompImportData) error {
	for _, rset := range data.Rulesets {
		if rset.FsetName == nil {
			continue
		}
		name := *rset.RulesetName
		rsetID := ci.rsets[name]
		rel := name + " -> " + *rset.FsetName
		fsetID, err := ci.filtersetID(ctx, *rset.FsetName)
		if err != nil {
			return err
		}
		if found, err := ci.oDb.exists(ctx, "importRulesetFiltersets",
			"SELECT 1 FROM comp_rulesets_filtersets WHERE ruleset_id = ? AND fset_id = ?", rsetID, fsetID); err != nil {
			return err
		} else if found {
			ci.add("Ruleset filterset relation already exists: %s", rel)
			continue
		}
		if err := ci.mayChange(ctx, CompRulesetKind, name, rsetID); err != nil {
			return err
		}
		if err := ci.oDb.SetCompRulesetFilterset(ctx, rsetID, int64(fsetID)); err != nil {
			return err
		}
		ci.add("Ruleset filterset relation added: %s", rel)
	}
	return nil
}

func (ci *compImport) importRulesetRelations(ctx context.Context, data CompImportData) error {
	for _, rset := range data.Rulesets {
		name := *rset.RulesetName
		parent := ci.rsets[name]
		for _, childName := range rset.Rulesets {
			rel := name + " -> " + childName
			child, err := ci.rulesetID(ctx, childName)
			if err != nil {
				return err
			}
			if found, err := ci.oDb.CompRulesetChildAttached(ctx, parent, child); err != nil {
				return err
			} else if found {
				ci.add("Ruleset relation already exists: %s", rel)
				continue
			}
			if err := ci.mayChange(ctx, CompRulesetKind, name, parent); err != nil {
				return err
			}
			if loop, err := ci.oDb.CompRulesetLoop(ctx, child, parent); err != nil {
				return err
			} else if loop || child == parent {
				return invalidf("the ruleset relation %s would cause an infinite recursion", rel)
			}
			if err := ci.oDb.AttachCompRulesetChild(ctx, parent, child); err != nil {
				return err
			}
			ci.add("Ruleset relation added: %s", rel)
		}
	}
	return nil
}

func (ci *compImport) importVariables(ctx context.Context, data CompImportData) error {
	for _, rset := range data.Rulesets {
		name := *rset.RulesetName
		rsetID := ci.rsets[name]
		for _, v := range rset.Variables {
			if v.VarName == nil || v.VarClass == nil || v.VarValue == nil {
				b, _ := json.Marshal(v)
				return invalidf("invalid variable format: %s", b)
			}
			desc := name + " :: " + *v.VarName + " (" + *v.VarClass + ")"
			var id int64
			var class, value sql.NullString
			err := ci.oDb.DB.QueryRowContext(ctx, "SELECT id, var_class, var_value FROM comp_rulesets_variables"+
				" WHERE ruleset_id = ? AND var_name = ? LIMIT 1", rsetID, *v.VarName).Scan(&id, &class, &value)
			found := err == nil
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("importVariables: %w", err)
			}
			if found && class.String == *v.VarClass && value.String == *v.VarValue {
				ci.add("Variable already exists: %s", desc)
				continue
			}
			if err := ci.mayChange(ctx, CompRulesetKind, name, rsetID); err != nil {
				return err
			}
			fields := map[string]any{"var_class": *v.VarClass, "var_value": *v.VarValue}
			if found {
				err = ci.oDb.UpdateCompRulesetVariable(ctx, id, fields, ci.imp.Author)
			} else {
				fields["var_name"] = *v.VarName
				_, err = ci.oDb.CreateCompRulesetVariable(ctx, rsetID, fields, ci.imp.Author)
			}
			if err != nil {
				return err
			}
			ci.add("Variable added: %s", desc)
		}
	}
	return nil
}

func (ci *compImport) importModulesets(ctx context.Context, data CompImportData) error {
	for _, modset := range data.Modulesets {
		if modset.ModsetName == nil || *modset.ModsetName == "" {
			return invalidf("invalid moduleset format: a modset_name is expected")
		}
		name := *modset.ModsetName
		id, found, err := ci.idByName(ctx, "comp_moduleset", "modset_name", name)
		if err != nil {
			return err
		}
		if found {
			ci.modsets[name] = id
			ci.existing[CompModulesetKind.Name+"/"+name] = true
			ci.add("Moduleset already exists: %s", name)
			continue
		}
		if id, err = ci.oDb.CreateCompModuleset(ctx, name, ci.imp.Author, ci.imp.GroupID); err != nil {
			return err
		}
		ci.modsets[name] = id
		ci.add("Moduleset added: %s", name)
	}
	return nil
}

func (ci *compImport) importModules(ctx context.Context, data CompImportData) error {
	for _, modset := range data.Modulesets {
		name := *modset.ModsetName
		modsetID := ci.modsets[name]
		for _, mod := range modset.Modules {
			if mod.ModsetModName == nil || *mod.ModsetModName == "" || mod.Autofix == nil {
				b, _ := json.Marshal(mod)
				return invalidf("invalid module format: %s", b)
			}
			desc := name + " :: " + *mod.ModsetModName + " (" + string(*mod.Autofix) + ")"
			var id int64
			var autofix sql.NullString
			err := ci.oDb.DB.QueryRowContext(ctx, "SELECT id, autofix FROM comp_moduleset_modules"+
				" WHERE modset_id = ? AND modset_mod_name = ? LIMIT 1", modsetID, *mod.ModsetModName).Scan(&id, &autofix)
			found := err == nil
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("importModules: %w", err)
			}
			if stored := autofix.String == "T" || autofix.String == "1"; found && stored == (*mod.Autofix == "T") {
				ci.add("Module already exists: %s", desc)
				continue
			}
			if err := ci.mayChange(ctx, CompModulesetKind, name, modsetID); err != nil {
				return err
			}
			fields := map[string]any{"autofix": string(*mod.Autofix)}
			if found {
				err = ci.oDb.UpdateCompModulesetModule(ctx, id, fields, ci.imp.Author)
			} else {
				fields["modset_mod_name"] = *mod.ModsetModName
				_, err = ci.oDb.CreateCompModulesetModule(ctx, modsetID, fields, ci.imp.Author)
			}
			if err != nil {
				return err
			}
			ci.add("Module added: %s", desc)
		}
	}
	return nil
}

func (ci *compImport) importModulesetRelations(ctx context.Context, data CompImportData) error {
	for _, modset := range data.Modulesets {
		name := *modset.ModsetName
		parent := ci.modsets[name]
		for _, childName := range modset.Modulesets {
			rel := name + " -> " + childName
			child, ok := ci.modsets[childName]
			if !ok {
				var found bool
				var err error
				if child, found, err = ci.idByName(ctx, "comp_moduleset", "modset_name", childName); err != nil {
					return err
				} else if !found {
					return invalidf("moduleset %s not found", childName)
				}
			}
			if found, err := ci.oDb.CompModulesetChildAttached(ctx, parent, child); err != nil {
				return err
			} else if found {
				ci.add("Moduleset relation already exists: %s", rel)
				continue
			}
			if err := ci.mayChange(ctx, CompModulesetKind, name, parent); err != nil {
				return err
			}
			if loop, err := ci.oDb.CompModulesetLoop(ctx, child, parent); err != nil {
				return err
			} else if loop || child == parent {
				return invalidf("the moduleset relation %s would cause an infinite recursion", rel)
			}
			if err := ci.oDb.AttachCompModulesetChild(ctx, parent, child); err != nil {
				return err
			}
			ci.add("Moduleset relation added: %s", rel)
		}
	}
	return nil
}

func (ci *compImport) importModulesetRulesets(ctx context.Context, data CompImportData) error {
	for _, modset := range data.Modulesets {
		name := *modset.ModsetName
		modsetID := ci.modsets[name]
		for _, rsetName := range modset.Rulesets {
			rel := name + " -> " + rsetName
			rsetID, err := ci.rulesetID(ctx, rsetName)
			if err != nil {
				return err
			}
			if found, err := ci.oDb.CompModulesetRulesetAttached(ctx, modsetID, rsetID); err != nil {
				return err
			} else if found {
				ci.add("Moduleset ruleset relation already exists: %s", rel)
				continue
			}
			if err := ci.mayChange(ctx, CompModulesetKind, name, modsetID); err != nil {
				return err
			}
			if err := ci.oDb.AttachCompModulesetRuleset(ctx, modsetID, rsetID); err != nil {
				return err
			}
			ci.add("Moduleset ruleset relation added: %s", rel)
		}
	}
	return nil
}
