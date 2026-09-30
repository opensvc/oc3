package serverhandlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v4"

	"github.com/opensvc/oc3/cdb"
	"github.com/opensvc/oc3/util/echolog"
)

// GetComplianceRulesetsExport handles GET /compliance/rulesets/export, as the
// historical rest_get_compliance_rulesets_export: the rulesets published to the
// caller, their descendants and their filtersets, in the format of
// POST /compliance/import.
func (a *Api) GetComplianceRulesetsExport(c echo.Context) error {
	ctx := c.Request().Context()
	ids, err := a.ODB.CompPublishedIDs(ctx, cdb.CompRulesetKind, UserGroupsFromContext(c), IsManager(c))
	if err != nil {
		return httpProblem(c, compDesignerError(c, "GetComplianceRulesetsExport", err))
	}
	return a.rulesetsExport(c, "GetComplianceRulesetsExport", ids)
}

// GetComplianceRulesetExport handles GET /compliance/rulesets/{rset_id}/export:
// a ruleset published to the caller, with its descendants and its filterset.
func (a *Api) GetComplianceRulesetExport(c echo.Context, rsetId string) error {
	id, err := a.publishedRuleset(c, rsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	return a.rulesetsExport(c, "GetComplianceRulesetExport", []int64{id})
}

func (a *Api) rulesetsExport(c echo.Context, name string, ids []int64) error {
	data, err := a.ODB.ExportCompRulesets(c.Request().Context(), ids)
	if err != nil {
		return httpProblem(c, compDesignerError(c, name, err))
	}
	return c.JSON(http.StatusOK, data)
}

// GetComplianceModulesetsExport handles GET /compliance/modulesets/export, as
// the historical rest_get_compliance_modulesets_export: the modulesets published
// to the caller, their descendants, and the rulesets and filtersets they use.
func (a *Api) GetComplianceModulesetsExport(c echo.Context) error {
	ctx := c.Request().Context()
	ids, err := a.ODB.CompPublishedIDs(ctx, cdb.CompModulesetKind, UserGroupsFromContext(c), IsManager(c))
	if err != nil {
		return httpProblem(c, compDesignerError(c, "GetComplianceModulesetsExport", err))
	}
	return a.modulesetsExport(c, "GetComplianceModulesetsExport", ids)
}

// GetComplianceModulesetExport handles GET /compliance/modulesets/{modset_id}/export:
// a moduleset published to the caller, with its descendants and their rulesets.
func (a *Api) GetComplianceModulesetExport(c echo.Context, modsetId string) error {
	id, err := a.publishedModuleset(c, modsetId)
	if err != nil {
		return httpProblem(c, err)
	}
	return a.modulesetsExport(c, "GetComplianceModulesetExport", []int64{id})
}

func (a *Api) modulesetsExport(c echo.Context, name string, ids []int64) error {
	data, err := a.ODB.ExportCompModulesets(c.Request().Context(), ids)
	if err != nil {
		return httpProblem(c, compDesignerError(c, name, err))
	}
	return c.JSON(http.StatusOK, data)
}

// PostComplianceImport handles POST /compliance/import, as the historical
// rest_post_compliance_import: the filters, filtersets, rulesets and modulesets
// of an export are created, or reused when they exist by name, and their
// relations added. Unlike the historical import, it requires the CompManager
// privilege, adds to an existing ruleset or moduleset only when the caller is
// responsible for it, and applies all or nothing.
func (a *Api) PostComplianceImport(c echo.Context) error {
	const name = "PostComplianceImport"
	if err := checkCompManager(c); err != nil {
		return httpProblem(c, err)
	}
	var data cdb.CompImportData
	if err := json.NewDecoder(c.Request().Body).Decode(&data); err != nil {
		return httpProblem(c, httpErrorf(http.StatusBadRequest, "invalid request body: %s", err))
	}
	ctx := c.Request().Context()
	caller, err := a.formCaller(ctx, c)
	if err != nil {
		return httpProblem(c, compDesignerError(c, name, err))
	}
	groupID, err := a.ODB.DefaultGroupOrManager(ctx, authUserID(c))
	if err != nil {
		return httpProblem(c, compDesignerError(c, name, err))
	}
	log := echolog.GetLogHandler(c, name)
	tx, markSuccess, endTx, err := a.ODB.BeginTxWithControl(ctx, log, &sql.TxOptions{})
	if err != nil {
		return httpProblem(c, httpInternal(log, "cannot start transaction", err))
	}
	groups, isManager := UserGroupsFromContext(c), IsManager(c)
	messages, err := tx.ImportCompliance(ctx, data, cdb.CompImporter{
		Author:  caller.name,
		GroupID: groupID,
		Responsible: func(ctx context.Context, k cdb.CompKind, id int64) (bool, error) {
			return tx.CompObjectResponsible(ctx, k, id, groups, isManager)
		},
	})
	if err == nil {
		markSuccess()
	}
	endTx()
	if err != nil {
		return httpProblem(c, compDesignerError(c, name, err))
	}
	a.compLog(c, "compliance.import", "imported compliance designer data (%(n)s items)",
		map[string]any{"n": len(messages)}, "", "")
	a.afterRulesetChange(c, true)
	return c.JSON(http.StatusOK, map[string][]string{"info": messages})
}
